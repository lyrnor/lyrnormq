//! Replica values shared by storage backends and authenticated RPC.
use mqtt_cluster_core::{ClusterMessage, DeliveryId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RepairToken {
    pub id: String,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RepairManifest {
    pub records: u64,
    pub bytes: u64,
    pub digest: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RepairEntry {
    pub key: String,
    pub min_epoch: u64,
    pub record: Option<Record>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RepairPage {
    pub entries: Vec<RepairEntry>,
    pub next: Option<String>,
    pub done: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RepairStoreStatus {
    pub disk_id: String,
    pub generation: u64,
    pub frozen: Option<RepairToken>,
    pub next_page: u64,
    pub manifest: Option<RepairManifest>,
    pub installed: bool,
}
/// FIFO source streams retain a high water mark rather than one receipt per message.
#[derive(Clone, Default, Serialize)]
#[serde(transparent)]
pub(crate) struct Receipts(BTreeMap<String, DeliveryId>);
impl Receipts {
    pub fn contains(&self, id: &DeliveryId) -> bool {
        self.0.get(&id.source).is_some_and(|highest| highest >= id)
    }
    pub fn insert(&mut self, id: DeliveryId) {
        let entry = self
            .0
            .entry(id.source.clone())
            .or_insert_with(|| id.clone());
        if *entry < id {
            *entry = id;
        }
    }
    pub fn clear(&mut self) {
        self.0.clear();
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}
impl<'de> Deserialize<'de> for Receipts {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Representation {
            Watermarks(BTreeMap<String, DeliveryId>),
            Legacy(Vec<DeliveryId>),
        }
        let mut result = Self::default();
        match Representation::deserialize(deserializer)? {
            Representation::Watermarks(values) => {
                if values.iter().any(|(source, id)| source != &id.source) {
                    return Err(serde::de::Error::custom("receipt stream identity mismatch"));
                }
                result.0 = values;
            }
            Representation::Legacy(values) => {
                for id in values {
                    result.insert(id);
                }
            }
        }
        if result.len() > 8192 {
            return Err(serde::de::Error::custom("receipt stream capacity"));
        }
        Ok(result)
    }
}
#[derive(Clone, Serialize)]
pub(crate) struct Recipient {
    pub client_id: String,
    pub generation: u64,
}
impl<'de> Deserialize<'de> for Recipient {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Current {
                client_id: String,
                #[serde(default)]
                generation: u64,
            },
            Legacy(String),
        }
        Ok(match Value::deserialize(deserializer)? {
            Value::Current {
                client_id,
                generation,
            } => Self {
                client_id,
                generation,
            },
            Value::Legacy(client_id) => Self {
                client_id,
                generation: 0,
            },
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Intent {
    pub id: DeliveryId,
    pub message: ClusterMessage,
    pub recipients: Vec<Recipient>,
    pub completed: BTreeSet<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Record {
    pub epoch: u64,
    pub physical_revision: u64,
    pub revision: u64,
    #[serde(default)]
    pub logical_generation: u64,
    pub snapshot: Option<Vec<u8>>,
    pub filters: Vec<String>,
    pub intents: Vec<Intent>,
    pub receipts: Receipts,
}
impl Record {
    pub fn received_or_obsolete(&self, id: &DeliveryId) -> bool {
        self.logical_generation != id.target_generation || self.receipts.contains(id)
    }
    pub fn version(&self) -> (u64, u64) {
        (self.epoch, self.physical_revision)
    }
    pub fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            physical_revision: 0,
            revision: 0,
            logical_generation: 0,
            snapshot: None,
            filters: Vec::new(),
            intents: Vec::new(),
            receipts: Receipts::default(),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Request {
    Read { key: String },
    Fence { key: String, epoch: u64 },
    Write { key: String, record: Record },
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Reply {
    pub record: Option<Record>,
}
