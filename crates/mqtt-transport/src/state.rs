//! Live connection registry and routing state.
use crate::packet_io::invalid;
use mqtt_protocol::Packet;
use mqtt_router::Router;
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::{mpsc, watch};

pub(super) struct Delivery {
    pub(super) publication: Arc<Packet>,
    pub(super) qos: u8,
}
pub(super) struct Client {
    pub(super) outgoing: mpsc::Sender<Delivery>,
    pub(super) cancel: watch::Sender<bool>,
}
#[derive(Default)]
pub(super) struct State {
    pub(super) router: Router,
    pub(super) clients: HashMap<u64, Client>,
    pub(super) identities: HashMap<String, u64>,
}
pub(super) fn lock(state: &Mutex<State>) -> io::Result<MutexGuard<'_, State>> {
    state
        .lock()
        .map_err(|_| io::Error::other("broker state poisoned"))
}
pub(super) fn active(state: &State, generation: u64) -> io::Result<()> {
    if state.clients.contains_key(&generation) {
        Ok(())
    } else {
        Err(invalid("connection superseded"))
    }
}
pub(super) fn remove(state: &mut State, generation: u64) {
    state.router.remove_client(generation);
    state.clients.remove(&generation);
    state.identities.retain(|_, current| *current != generation);
}
/// The publication is shared among matching recipients. Each connection assigns
/// its own outbound identifier only when it drains a delivery from its queue.
pub(super) fn route(state: &Mutex<State>, generation: u64, packet: Packet) -> io::Result<()> {
    let Packet::Publish { topic, qos, .. } = &packet else {
        return Err(invalid("routing requires a PUBLISH"));
    };
    let mut state = lock(state)?;
    active(&state, generation)?;
    let targets = state.router.matching_qos(topic);
    let published_qos = *qos;
    let publication = Arc::new(packet);
    let mut evict = Vec::new();
    for (target, granted_qos) in targets {
        if let Some(client) = state.clients.get(&target)
            && client
                .outgoing
                .try_send(Delivery {
                    publication: Arc::clone(&publication),
                    qos: published_qos.min(granted_qos),
                })
                .is_err()
        {
            let _ = client.cancel.send(true);
            evict.push(target);
        }
    }
    for target in evict {
        remove(&mut state, target);
    }
    Ok(())
}
