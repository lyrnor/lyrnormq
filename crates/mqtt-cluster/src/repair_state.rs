//! Durable metadata-only repair authority and maintenance transitions.
use crate::model::{DataMode, Metadata};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairSeal {
    pub records: u64,
    pub bytes: u64,
    pub digest: String,
}
impl RepairSeal {
    pub(crate) fn valid(&self) -> bool {
        self.records <= 101_024
            && self.bytes <= 768 * 1024 * 1024
            && self.digest.len() == 64
            && self.digest.bytes().all(|b| b.is_ascii_hexdigit())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairState {
    pub id: String,
    pub target: u64,
    pub old_disk: String,
    pub new_disk: String,
    pub donors: Vec<(u64, String)>,
    pub generation: u64,
    pub manifest: Option<RepairSeal>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepairOutcome {
    Committed,
    Aborted,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairTerminal {
    pub repair: RepairState,
    pub outcome: RepairOutcome,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairProof {
    pub node: u64,
    pub disk_id: String,
    pub id: String,
    pub generation: u64,
    pub manifest: Option<RepairSeal>,
    pub installed: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepairSummary {
    pub id: String,
    pub target: u64,
    pub generation: u64,
    pub sealed: bool,
    pub state: String,
}
impl RepairState {
    pub fn summary(&self, state: &str) -> RepairSummary {
        RepairSummary {
            id: self.id.clone(),
            target: self.target,
            generation: self.generation,
            sealed: self.manifest.is_some(),
            state: state.to_owned(),
        }
    }
}
impl Metadata {
    pub(crate) fn begin_repair(
        &mut self,
        target: u64,
        old_disk: String,
        new_disk: String,
        expected_generation: u64,
    ) -> Result<(), String> {
        if let Some(active) = &self.repair {
            return if active.target == target
                && active.old_disk == old_disk
                && active.new_disk == new_disk
            {
                Ok(())
            } else {
                Err("another repair is active".into())
            };
        }
        if expected_generation != self.data_generation
            || self.disk_ids.get(&target) != Some(&old_disk)
            || self.nodes.contains_key(&target)
            || new_disk.len() != 32
            || !new_disk.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("repair precondition changed or target still leased".into());
        }
        let peers = match self
            .data_mode
            .as_ref()
            .and_then(DataMode::peers)
            .filter(|peers| peers.iter().any(|(id, _)| *id == target))
        {
            Some(peers) => peers,
            None => return Err("durable replica membership required".into()),
        };
        if peers.len() != 3 {
            return Err("repair requires 3 data replicas".into());
        }
        let donors = peers
            .iter()
            .filter(|(id, _)| *id != target)
            .map(|(id, _)| {
                if self.excluded.contains(id) || !self.nodes.contains_key(id) {
                    return Err("two original donors required".to_owned());
                }
                self.disk_ids
                    .get(id)
                    .cloned()
                    .map(|disk| (*id, disk))
                    .ok_or_else(|| "donor disk identity missing".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if donors.len() != 2 {
            return Err("two original donors required".into());
        }
        self.data_generation = self
            .data_generation
            .checked_add(1)
            .ok_or("data generation exhausted")?;
        self.excluded.insert(target);
        self.repair = Some(RepairState {
            id: format!(
                "repair-{}",
                self.revision
                    .checked_add(1)
                    .ok_or("repair revision exhausted")?
            ),
            target,
            old_disk,
            new_disk,
            donors,
            generation: self.data_generation,
            manifest: None,
        });
        Ok(())
    }
    pub(crate) fn seal_repair(
        &mut self,
        id: &str,
        generation: u64,
        manifest: RepairSeal,
    ) -> Result<(), String> {
        let repair = self.repair.as_mut().ok_or("no active repair")?;
        if repair.id != id || repair.generation != generation || !manifest.valid() {
            return Err("invalid repair seal".into());
        }
        if repair.manifest.as_ref().is_some_and(|old| old != &manifest) {
            return Err("repair seal conflicts".into());
        }
        repair.manifest = Some(manifest);
        Ok(())
    }
    pub(crate) fn commit_repair(
        &mut self,
        id: &str,
        generation: u64,
        proofs: Vec<RepairProof>,
    ) -> Result<(), String> {
        let repair = self.repair.as_ref().ok_or("no active repair")?;
        if repair.id != id || repair.generation != generation || proofs.len() != 3 {
            return Err("repair token changed".into());
        }
        let expected = repair.manifest.as_ref().ok_or("repair not sealed")?;
        for (node, disk) in repair
            .donors
            .iter()
            .chain(std::iter::once(&(repair.target, repair.new_disk.clone())))
        {
            let proof = proofs
                .iter()
                .find(|p| p.node == *node)
                .ok_or("repair proof missing")?;
            if proof.disk_id != *disk || proof.id != id || proof.generation != generation {
                return Err("repair proof mismatch".into());
            }
            if *node == repair.target
                && (!proof.installed || proof.manifest.as_ref() != Some(expected))
            {
                return Err("target not fully installed".into());
            }
        }
        let repair = self.repair.take().ok_or("no active repair")?;
        self.disk_ids.insert(repair.target, repair.new_disk.clone());
        self.excluded.remove(&repair.target);
        self.last_repair = Some(RepairTerminal {
            repair,
            outcome: RepairOutcome::Committed,
        });
        Ok(())
    }
    pub(crate) fn abort_repair(&mut self, id: &str, generation: u64) -> Result<(), String> {
        let active = self.repair.as_ref().ok_or("no active repair")?;
        if active.id != id || active.generation != generation {
            return Err("repair token changed".into());
        }
        let repair = self.repair.take().ok_or("no active repair")?;
        self.last_repair = Some(RepairTerminal {
            repair,
            outcome: RepairOutcome::Aborted,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Command, Node};
    fn configured() -> Metadata {
        let mut m = Metadata {
            data_mode: Some(DataMode::Durable(
                (1..=3)
                    .map(|i| (i, format!("http://127.0.0.1:{}", 7000 + i)))
                    .collect(),
            )),
            ..Metadata::default()
        };
        for id in 1..=3 {
            m.disk_ids.insert(id, format!("{id:032x}"));
        }
        for id in 2..=3 {
            m.nodes.insert(
                id,
                Node {
                    id,
                    incarnation: format!("boot-{id}"),
                    address: format!("http://127.0.0.1:{}", 7000 + id),
                    expires_at: 100,
                    max_lease_ms: 3000,
                    applied: 0,
                },
            );
        }
        m
    }
    fn begin(m: &mut Metadata) {
        assert!(
            m.apply(Command::RepairBegin {
                target: 1,
                old_disk: format!("{:032x}", 1),
                new_disk: format!("{:032x}", 4),
                expected_generation: m.data_generation
            })
            .error
            .is_none()
        );
    }
    fn manifest() -> RepairSeal {
        RepairSeal {
            records: 2,
            bytes: 64,
            digest: "a".repeat(64),
        }
    }
    #[test]
    fn begin_abort_never_reenables_unrepaired_target_or_loses_old_anchor() {
        let mut m = configured();
        begin(&mut m);
        let token = m.repair.clone().expect("active");
        assert_eq!(m.data_generation, 1);
        assert!(m.excluded.contains(&1));
        assert!(
            m.apply(Command::Acquire {
                key: "c:client".into(),
                node: 2,
                incarnation: "boot-2".into(),
                generation: 1,
                expected_epoch: 0
            })
            .error
            .is_some()
        );
        assert!(
            m.apply(Command::RepairAbort {
                id: token.id.clone(),
                generation: 1
            })
            .error
            .is_none()
        );
        assert!(m.excluded.contains(&1));
        assert_eq!(m.disk_ids[&1], format!("{:032x}", 1));
        assert_eq!(
            m.last_repair.as_ref().expect("terminal").outcome,
            RepairOutcome::Aborted
        );
        begin(&mut m);
        assert_eq!(m.data_generation, 2);
        assert!(
            m.apply(Command::RepairAbort {
                id: token.id,
                generation: 1
            })
            .error
            .is_some()
        );
    }
    #[test]
    fn commit_requires_exact_original_proofs_and_installed_sealed_target() {
        let mut m = configured();
        begin(&mut m);
        let repair = m.repair.clone().expect("repair");
        assert!(
            m.apply(Command::RepairSeal {
                id: repair.id.clone(),
                generation: 1,
                manifest: manifest()
            })
            .error
            .is_none()
        );
        let mut proofs = vec![];
        for node in [2, 3, 1] {
            proofs.push(RepairProof {
                node,
                disk_id: format!("{:032x}", if node == 1 { 4 } else { node }),
                id: repair.id.clone(),
                generation: 1,
                manifest: Some(manifest()),
                installed: node == 1,
            });
        }
        let mut wrong = proofs.clone();
        wrong[2].installed = false;
        assert!(
            m.apply(Command::RepairCommit {
                id: repair.id.clone(),
                generation: 1,
                proofs: wrong
            })
            .error
            .is_some()
        );
        let old = m.clone();
        assert!(
            m.apply(Command::RepairCommit {
                id: repair.id,
                generation: 1,
                proofs: std::mem::take(&mut proofs)
            })
            .error
            .is_none()
        );
        assert!(!m.excluded.contains(&1));
        assert_eq!(m.disk_ids[&1], format!("{:032x}", 4));
        assert!(m.repair.is_none());
        let merged = crate::model::MetadataUpdate::between(&old, &m)
            .merge(&old)
            .expect("delta");
        assert_eq!(merged.data_generation, m.data_generation);
        assert_eq!(merged.last_repair, m.last_repair);
    }
}
