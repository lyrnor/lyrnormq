use crate::{
    LoadError,
    model::{PairData, Sample},
    payload::Header,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    AwaitAck,
    AwaitRec,
    AwaitComp,
    Done,
}
#[derive(Debug)]
pub(super) struct Pending {
    pub header: Header,
    pub sample: Option<usize>,
    pub source: Source,
    pub delivered: bool,
    pub wrote: bool,
}
pub(super) struct State {
    pub data: PairData,
    pub pending: BTreeMap<u64, Pending>,
    pub outgoing: BTreeMap<u16, u64>,
    pub incoming: BTreeMap<u16, Header>,
    pub incoming_done: BTreeSet<u16>,
    pub next_id: u16,
    pub next_sequence: u64,
    pub warm_sent: u64,
    pub measured_sent: u64,
    pub first_measured: Option<u64>,
}
impl State {
    pub fn new(pair: u32, setup_duration_ns: u64) -> Self {
        Self {
            data: PairData {
                pair,
                setup_duration_ns,
                ..PairData::default()
            },
            pending: BTreeMap::new(),
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
            incoming_done: BTreeSet::new(),
            next_id: 1,
            next_sequence: 0,
            warm_sent: 0,
            measured_sent: 0,
            first_measured: None,
        }
    }
    pub fn allocate_id(&mut self) -> Result<u16, LoadError> {
        for _ in 0..u16::MAX {
            let id = self.next_id;
            self.next_id = if id == u16::MAX { 1 } else { id + 1 };
            if !self.outgoing.contains_key(&id) {
                return Ok(id);
            }
        }
        Err(LoadError::Capacity("packet identifiers"))
    }
    pub fn begin(&mut self, header: Header, qos: u8, id: Option<u16>) -> Result<(), LoadError> {
        let sample = if header.measured {
            self.first_measured.get_or_insert(header.sequence);
            let index = self.data.samples.len();
            self.data
                .samples
                .try_reserve(1)
                .map_err(|_| LoadError::Capacity("sample allocation"))?;
            self.data.samples.push(Sample {
                pair: header.pair,
                sequence: header.sequence,
                scheduled_ns: header.scheduled_ns,
                send_started_ns: header.send_started_ns,
                write_completed_ns: None,
                ack_ns: None,
                delivery_ns: None,
                completed_ns: None,
            });
            self.data.counters.attempted += 1;
            Some(index)
        } else {
            None
        };
        if let Some(id) = id {
            self.outgoing.insert(id, header.sequence);
        }
        self.pending.insert(
            header.sequence,
            Pending {
                header,
                sample,
                source: match qos {
                    0 => Source::Done,
                    1 => Source::AwaitAck,
                    _ => Source::AwaitRec,
                },
                delivered: false,
                wrote: false,
            },
        );
        Ok(())
    }
    pub fn wrote(&mut self, seq: u64, now: u64) -> Result<(), LoadError> {
        let record = self
            .pending
            .get_mut(&seq)
            .ok_or_else(|| LoadError::InvalidData("write without pending record".into()))?;
        record.wrote = true;
        if let Some(i) = record.sample {
            self.data.samples[i].write_completed_ns = Some(now);
            self.data.counters.write_completed += 1;
        }
        self.finish(seq, now);
        Ok(())
    }
    pub fn source_ack(&mut self, id: u16, kind: u8, now: u64) -> Result<bool, LoadError> {
        let seq = *self.outgoing.get(&id).ok_or_else(|| {
            LoadError::InvalidData(format!("unexpected source acknowledgement {kind} id{id}"))
        })?;
        let p = self
            .pending
            .get_mut(&seq)
            .ok_or_else(|| LoadError::InvalidData("ack without pending".into()))?;
        match (p.source, kind) {
            (Source::AwaitAck, 4) => {
                p.source = Source::Done;
                if p.header.measured {
                    self.data.counters.puback_received += 1;
                }
            }
            (Source::AwaitRec, 5) => {
                p.source = Source::AwaitComp;
                if p.header.measured {
                    self.data.counters.pubrec_received += 1;
                }
                return Ok(true);
            }
            (Source::AwaitComp, 5) => return Ok(true),
            (Source::AwaitComp, 7) => {
                p.source = Source::Done;
                if p.header.measured {
                    self.data.counters.pubcomp_received += 1;
                }
            }
            _ => {
                return Err(LoadError::InvalidData(format!(
                    "source ACK {kind} invalid in {:?}",
                    p.source
                )));
            }
        }
        if let Some(i) = p.sample {
            self.data.samples[i].ack_ns = Some(now);
        }
        self.outgoing.remove(&id);
        self.finish(seq, now);
        Ok(false)
    }
    pub fn validate_header(&self, h: Header, now: u64) -> Result<(), LoadError> {
        if h.pair != self.data.pair || h.scheduled_ns > h.send_started_ns || h.send_started_ns > now
        {
            return Err(LoadError::InvalidData(
                "payload pair/timestamp mismatch".into(),
            ));
        }
        if let Some(p) = self.pending.get(&h.sequence) {
            if p.header != h {
                return Err(LoadError::InvalidData(
                    "payload differs from sent header".into(),
                ));
            }
            return Ok(());
        }
        if !h.measured && self.first_measured.is_some_and(|first| h.sequence >= first) {
            return Err(LoadError::InvalidData(
                "duplicate cohort flag mismatch".into(),
            ));
        }
        if h.measured {
            let first = self
                .first_measured
                .ok_or_else(|| LoadError::InvalidData("unknown measured sequence".into()))?;
            let index = h
                .sequence
                .checked_sub(first)
                .and_then(|x| usize::try_from(x).ok())
                .ok_or_else(|| LoadError::InvalidData("unknown measured sequence".into()))?;
            let s = self
                .data
                .samples
                .get(index)
                .ok_or_else(|| LoadError::InvalidData("unknown measured sequence".into()))?;
            if s.sequence != h.sequence
                || s.scheduled_ns != h.scheduled_ns
                || s.send_started_ns != h.send_started_ns
            {
                return Err(LoadError::InvalidData("duplicate header mismatch".into()));
            }
        }
        if h.sequence >= self.next_sequence {
            return Err(LoadError::InvalidData("unsent payload sequence".into()));
        }
        Ok(())
    }
    pub fn deliver(&mut self, h: Header, now: u64) -> Result<(), LoadError> {
        self.validate_header(h, now)?;
        if let Some(p) = self.pending.get_mut(&h.sequence) {
            if p.delivered {
                if h.measured {
                    self.data.counters.duplicates += 1;
                }
                return Ok(());
            }
            p.delivered = true;
            if let Some(i) = p.sample {
                self.data.samples[i].delivery_ns = Some(now);
                self.data.counters.delivered_unique += 1;
            }
            self.finish(h.sequence, now);
            return Ok(());
        }
        if h.measured {
            self.data.counters.duplicates += 1;
        }
        Ok(())
    }
    fn finish(&mut self, seq: u64, now: u64) {
        let finished = self
            .pending
            .get(&seq)
            .is_some_and(|p| p.wrote && p.delivered && p.source == Source::Done);
        if finished
            && let Some(p) = self.pending.remove(&seq)
            && let Some(i) = p.sample
        {
            self.data.samples[i].completed_ns = Some(now);
            self.data.counters.completed += 1;
        }
    }
}
