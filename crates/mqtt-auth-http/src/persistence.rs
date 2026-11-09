use crate::Error;
use mqtt_storage::configuration::ConfigStore;
use std::{path::PathBuf, sync::mpsc};
use tokio::sync::{oneshot, watch};
enum Command {
    Save(Vec<u8>, oneshot::Sender<Result<(), Error>>),
    Close,
}
pub(crate) struct Owner {
    sender: mpsc::SyncSender<Command>,
    done: watch::Receiver<bool>,
    #[cfg(test)]
    writes: std::sync::Arc<std::sync::atomic::AtomicU64>,
}
impl Owner {
    pub fn open(path: PathBuf) -> Result<(Self, Option<Vec<u8>>), Error> {
        let (sender, receiver) = mpsc::sync_channel(4);
        let (ready, opened) = mpsc::sync_channel(1);
        let (done, finished) = watch::channel(false);
        #[cfg(test)]
        let writes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let worker_writes = writes.clone();
        std::thread::Builder::new()
            .name("lyrnormq-access-config".into())
            .spawn(move || {
                let result = ConfigStore::open(&path, super::config::MAX_CONFIG)
                    .map_err(|_| Error::Storage)
                    .and_then(|store| {
                        let bytes = store.load().map_err(|_| Error::Storage)?;
                        Ok((store, bytes))
                    });
                match result {
                    Ok((mut store, bytes)) => {
                        if ready.send(Ok(bytes)).is_ok() {
                            while let Ok(command) = receiver.recv() {
                                match command {
                                    Command::Save(bytes, reply) => {
                                        #[cfg(test)]
                                        worker_writes
                                            .fetch_add(1, std::sync::atomic::Ordering::Release);
                                        let result = store.save(&bytes).map_err(|_| Error::Storage);
                                        let _ = reply.send(result);
                                    }
                                    Command::Close => break,
                                }
                            }
                        }
                        drop(store);
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                }
                done.send_replace(true);
            })
            .map_err(|_| Error::Storage)?;
        let bytes = opened.recv().map_err(|_| Error::Storage)??;
        Ok((
            Self {
                sender,
                done: finished,
                #[cfg(test)]
                writes,
            },
            bytes,
        ))
    }
    #[cfg(test)]
    pub fn writes(&self) -> u64 {
        self.writes.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn save(&self, bytes: Vec<u8>) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .try_send(Command::Save(bytes, tx))
            .map_err(|_| Error::Storage)?;
        rx.await.map_err(|_| Error::Storage)?
    }
    pub async fn close(&self) {
        let _ = self.sender.try_send(Command::Close);
        let mut done = self.done.clone();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !*done.borrow_and_update() {
                if done.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.sender.try_send(Command::Close);
    }
}
