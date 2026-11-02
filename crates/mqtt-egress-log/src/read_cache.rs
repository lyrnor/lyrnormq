//! Bounded advisory immutable-body cache. Durable indexes remain authoritative.
#[cfg(unix)]
mod platform {
    use crate::{QueueError, shared::Body, storage};
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs::{File, Metadata},
        path::Path,
        time::SystemTime,
    };

    const BODY_BYTES: usize = 8 * 1024 * 1024;
    const BODY_ENTRIES: usize = 8192;
    const DESCRIPTORS: usize = 16;
    #[derive(Clone, PartialEq, Eq)]
    struct Fingerprint {
        len: u64,
        modified: SystemTime,
        #[cfg(unix)]
        identity: (u64, u64, i64, i64, i64, i64),
    }
    impl Fingerprint {
        fn new(m: &Metadata) -> Result<Self, QueueError> {
            Ok(Self {
                len: m.len(),
                modified: m.modified()?,
                #[cfg(unix)]
                identity: (
                    m.dev(),
                    m.ino(),
                    m.mtime(),
                    m.mtime_nsec(),
                    m.ctime(),
                    m.ctime_nsec(),
                ),
            })
        }
    }
    struct Segment {
        file: File,
        fingerprint: Fingerprint,
    }
    struct CachedBody {
        segment: u64,
        fingerprint: Fingerprint,
        payload: Vec<u8>,
        ticket: u64,
    }
    #[derive(Default)]
    pub(crate) struct ReadCache {
        segments: BTreeMap<u64, Segment>,
        bodies: BTreeMap<u64, CachedBody>,
        order: BTreeMap<u64, u64>,
        bytes: usize,
        ticket: u64,
        #[cfg(test)]
        hits: usize,
    }
    impl ReadCache {
        pub(crate) fn clear(&mut self) {
            self.segments.clear();
            self.bodies.clear();
            self.order.clear();
            self.bytes = 0;
            self.ticket = 0;
        }
        pub(crate) fn remove(&mut self, result: u64) {
            if let Some(old) = self.bodies.remove(&result) {
                self.bytes -= old.payload.len();
                self.order.remove(&old.ticket);
            }
        }
        fn invalidate_segment(&mut self, segment: u64) {
            let ids: Vec<_> = self
                .bodies
                .iter()
                .filter_map(|(id, b)| (b.segment == segment).then_some(*id))
                .collect();
            for id in ids {
                self.remove(id);
            }
        }
        fn prepare(&mut self, dir: &Path, segment: u64) -> Result<(), QueueError> {
            let path = storage::segment(dir, segment);
            if let Some(open) = self.segments.get(&segment) {
                let checked =
                    storage::private_metadata(&open.file, &path).and_then(|m| Fingerprint::new(&m));
                match checked {
                    Ok(fingerprint) => {
                        if fingerprint != open.fingerprint {
                            self.invalidate_segment(segment);
                            if let Some(open) = self.segments.get_mut(&segment) {
                                open.fingerprint = fingerprint;
                            }
                        }
                        return Ok(());
                    }
                    Err(error) => {
                        self.invalidate_segment(segment);
                        self.segments.remove(&segment);
                        return Err(error);
                    }
                }
            }
            let file = storage::private_file(&path, false)?;
            let fingerprint = Fingerprint::new(&file.metadata()?)?;
            if self.segments.len() >= DESCRIPTORS
                && let Some(old) = self.segments.keys().next().copied()
            {
                self.segments.remove(&old);
            }
            self.segments.insert(segment, Segment { file, fingerprint });
            Ok(())
        }
        pub(crate) fn read(
            &mut self,
            dir: &Path,
            body: &Body,
            validated: &mut BTreeSet<u64>,
        ) -> Result<Vec<u8>, QueueError> {
            if !validated.contains(&body.segment) || !self.segments.contains_key(&body.segment) {
                self.prepare(dir, body.segment)?;
                validated.insert(body.segment);
            }
            let open = self.segments.get(&body.segment).ok_or(QueueError::Io)?;
            if let Some(cached) = self.bodies.get(&body.meta.id) {
                if cached.segment == body.segment && cached.fingerprint == open.fingerprint {
                    #[cfg(test)]
                    {
                        self.hits += 1;
                    }
                    return Ok(cached.payload.clone());
                }
                self.invalidate_segment(body.segment);
            }
            let open = self.segments.get(&body.segment).ok_or(QueueError::Io)?;
            let fingerprint = open.fingerprint.clone();
            let payload = storage::read_payload_open(
                &open.file,
                body.offset,
                body.meta.payload_len,
                body.checksum,
                fingerprint.len,
            )?;
            if body.consumers.len() > 1 && payload.len() <= BODY_BYTES {
                while self.bytes + payload.len() > BODY_BYTES || self.bodies.len() >= BODY_ENTRIES {
                    let Some((&ticket, &result)) = self.order.first_key_value() else {
                        break;
                    };
                    self.remove(result);
                    // Defensive invariant: a cache entry never outlives its eviction ticket.
                    self.order.remove(&ticket);
                }
                if self.ticket == u64::MAX {
                    self.clear();
                }
                let ticket = self.ticket;
                self.ticket += 1;
                self.bytes += payload.len();
                self.bodies.insert(
                    body.meta.id,
                    CachedBody {
                        segment: body.segment,
                        fingerprint,
                        payload: payload.clone(),
                        ticket,
                    },
                );
                self.order.insert(ticket, body.meta.id);
            }
            Ok(payload)
        }
        #[cfg(test)]
        pub(crate) fn stats(&self) -> (usize, usize, usize, usize, usize) {
            (
                self.bodies.len(),
                self.bytes,
                self.segments.len(),
                self.hits,
                self.order.len(),
            )
        }
    }
}
#[cfg(not(unix))]
mod platform {
    use crate::{QueueError, shared::Body, storage};
    use std::{collections::BTreeSet, path::Path};
    #[derive(Default)]
    pub(crate) struct ReadCache;
    impl ReadCache {
        pub(crate) fn clear(&mut self) {}
        pub(crate) fn remove(&mut self, _: u64) {}
        pub(crate) fn read(
            &mut self,
            dir: &Path,
            body: &Body,
            _: &mut BTreeSet<u64>,
        ) -> Result<Vec<u8>, QueueError> {
            storage::read_payload(
                dir,
                body.segment,
                body.offset,
                body.meta.payload_len,
                body.checksum,
            )
        }
    }
}
pub(crate) use platform::ReadCache;
