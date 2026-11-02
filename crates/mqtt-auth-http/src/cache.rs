use crate::{AclQuery, CacheStatus, backend::Client};
use mqtt_auth::RemoteAccessError;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};
use tokio::sync::watch;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENTRIES: usize = 65536;
#[derive(Clone, Debug)]
pub(crate) struct Receipt {
    pub allowed: bool,
    pub epoch: u64,
    pub expires: Option<tokio::time::Instant>,
}
impl Receipt {
    pub fn valid(&self) -> bool {
        self.expires
            .is_none_or(|at| tokio::time::Instant::now() < at)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    generation: u64,
    query: AclQuery,
}
impl Key {
    fn size(&self) -> usize {
        self.query.size().saturating_mul(2).saturating_add(256)
    }
}
type Answer = Result<Receipt, RemoteAccessError>;
struct Flight {
    token: u64,
    reply: watch::Sender<Option<Answer>>,
}
struct State {
    epoch: u64,
    next: u64,
    entries: BTreeMap<Key, Receipt>,
    flights: BTreeMap<Key, Flight>,
    bytes: usize,
    hits: u64,
    misses: u64,
}
pub(crate) struct Cache {
    state: Mutex<State>,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                epoch: 1,
                next: 1,
                entries: BTreeMap::new(),
                flights: BTreeMap::new(),
                bytes: 0,
                hits: 0,
                misses: 0,
            }),
        }
    }
}
impl Cache {
    pub fn epoch(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .epoch
    }
    pub fn clear(&self) -> CacheStatus {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        s.epoch = s.epoch.saturating_add(1);
        s.entries.clear();
        for f in s.flights.values() {
            f.reply
                .send_replace(Some(Err(RemoteAccessError::Unavailable)));
        }
        s.flights.clear();
        s.bytes = 0;
        Self::stats(&s)
    }
    fn stats(s: &State) -> CacheStatus {
        CacheStatus {
            entries: s.entries.len(),
            bytes: s.bytes,
            hits: s.hits,
            misses: s.misses,
            inflight: s.flights.len(),
        }
    }
    pub fn status(&self) -> CacheStatus {
        Self::stats(&self.state.lock().unwrap_or_else(PoisonError::into_inner))
    }
    pub fn lookup(&self, generation: u64, query: &AclQuery) -> Option<Receipt> {
        let key = Key {
            generation,
            query: query.clone(),
        };
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(r) = s.entries.get(&key).cloned() {
            if r.valid() {
                s.hits = s.hits.saturating_add(1);
                return Some(r);
            }
            s.entries.remove(&key);
            s.bytes = s.bytes.saturating_sub(key.size());
        }
        s.misses = s.misses.saturating_add(1);
        None
    }
    pub async fn resolve(
        self: &Arc<Self>,
        generation: u64,
        epoch: u64,
        query: AclQuery,
        client: &Client,
        ttl: u64,
    ) -> Answer {
        let key = Key { generation, query };
        let (owner, mut receive) = {
            let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if s.epoch != epoch {
                return Err(RemoteAccessError::Unavailable);
            }
            if let Some(r) = s.entries.get(&key).filter(|r| r.valid()).cloned() {
                return Ok(r);
            }
            if let Some(f) = s.flights.get(&key) {
                (None, f.reply.subscribe())
            } else {
                if s.flights.len() >= 4096 || s.bytes.saturating_add(key.size()) > MAX_BYTES {
                    return Err(RemoteAccessError::Busy);
                }
                let token = s.next;
                s.next = s.next.wrapping_add(1);
                let (reply, receive) = watch::channel(None);
                s.bytes += key.size();
                s.flights.insert(key.clone(), Flight { token, reply });
                (
                    Some(FlightGuard {
                        cache: self.clone(),
                        key: key.clone(),
                        token,
                        epoch,
                    }),
                    receive,
                )
            }
        };
        if let Some(guard) = owner {
            let result = match client.authorize(&key.query, true).await {
                Ok(r) => match r.result {
                    "allow" | "deny" => Ok(Receipt {
                        allowed: r.result == "allow",
                        epoch,
                        expires: (ttl != 0)
                            .then(|| tokio::time::Instant::now() + Duration::from_secs(ttl)),
                    }),
                    _ => Err(if r.error_code == Some("busy") {
                        RemoteAccessError::Busy
                    } else {
                        RemoteAccessError::Unavailable
                    }),
                },
                Err(_) => Err(RemoteAccessError::Unavailable),
            };
            guard.complete(result.clone(), ttl);
            return result;
        }
        loop {
            if let Some(answer) = receive.borrow_and_update().clone() {
                return answer;
            }
            receive
                .changed()
                .await
                .map_err(|_| RemoteAccessError::Unavailable)?;
        }
    }
}
struct FlightGuard {
    cache: Arc<Cache>,
    key: Key,
    token: u64,
    epoch: u64,
}
impl FlightGuard {
    fn complete(self, answer: Answer, ttl: u64) {
        let mut s = self
            .cache
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if s.epoch != self.epoch {
            return;
        }
        if s.flights
            .get(&self.key)
            .is_none_or(|f| f.token != self.token)
        {
            return;
        }
        if let Some(f) = s.flights.remove(&self.key) {
            s.bytes = s.bytes.saturating_sub(self.key.size());
            f.reply.send_replace(Some(answer.clone()));
        }
        if ttl != 0
            && let Ok(receipt) = answer
        {
            while s.entries.len() >= MAX_ENTRIES
                || s.bytes.saturating_add(self.key.size()) > MAX_BYTES
            {
                if let Some((key, _)) = s.entries.pop_first() {
                    s.bytes = s.bytes.saturating_sub(key.size());
                } else {
                    break;
                }
            }
            if s.bytes.saturating_add(self.key.size()) <= MAX_BYTES
                && s.entries.insert(self.key.clone(), receipt).is_none()
            {
                s.bytes += self.key.size();
            }
        }
    }
}
impl Drop for FlightGuard {
    fn drop(&mut self) {
        let mut s = self
            .cache
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if s.flights
            .get(&self.key)
            .is_some_and(|f| f.token == self.token)
            && let Some(f) = s.flights.remove(&self.key)
        {
            s.bytes = s.bytes.saturating_sub(self.key.size());
            f.reply
                .send_replace(Some(Err(RemoteAccessError::Unavailable)));
        }
    }
}
