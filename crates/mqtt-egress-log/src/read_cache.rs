//! Bounded advisory immutable-body cache. Durable indexes remain authoritative.
#[cfg(unix)]
mod platform {
    use crate::{QueueError, shared::Body, storage};
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs::{File, Metadata},
        os::unix::fs::MetadataExt,
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
        identity: (u64, u64, i64, i64, i64, i64),
    }
    impl Fingerprint {
        fn new(m: &Metadata) -> Result<Self, QueueError> {
            Ok(Self {
                len: m.len(),
                modified: m.modified()?,
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
        fn same_file(&self, other: &Self) -> bool {
            self.identity.0 == other.identity.0 && self.identity.1 == other.identity.1
        }
    }
    struct Segment {
        file: File,
        fingerprint: Fingerprint,
    }
    struct CachedBody {
        segment: u64,
        payload: Vec<u8>,
        ticket: u64,
    }
    // One fingerprint per segment, independent of descriptor eviction. Updating a
    // trusted append-only prefix never scans all cached bodies.
    struct Trust {
        fingerprint: Fingerprint,
        bodies: BTreeSet<u64>,
    }
    pub(crate) struct AppendCheck {
        segment: u64,
        fingerprint: Fingerprint,
    }
    #[derive(Default)]
    pub(crate) struct ReadCache {
        segments: BTreeMap<u64, Segment>,
        bodies: BTreeMap<u64, CachedBody>,
        trusts: BTreeMap<u64, Trust>,
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
            self.trusts.clear();
            self.order.clear();
            self.bytes = 0;
            self.ticket = 0;
        }
        pub(crate) fn remove(&mut self, result: u64) {
            if let Some(old) = self.bodies.remove(&result) {
                self.bytes -= old.payload.len();
                self.order.remove(&old.ticket);
                if let Some(trust) = self.trusts.get_mut(&old.segment) {
                    trust.bodies.remove(&result);
                    if trust.bodies.is_empty() {
                        self.trusts.remove(&old.segment);
                    }
                }
            }
        }
        fn invalidate_segment(&mut self, segment: u64) {
            if let Some(trust) = self.trusts.remove(&segment) {
                for id in trust.bodies {
                    if let Some(body) = self.bodies.remove(&id) {
                        self.bytes -= body.payload.len();
                        self.order.remove(&body.ticket);
                    }
                }
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
            // A reopened descriptor cannot bless bodies verified against an older
            // incarnation or contents, even if its file name and length match.
            if self
                .trusts
                .get(&segment)
                .is_some_and(|t| t.fingerprint != fingerprint)
            {
                self.invalidate_segment(segment);
            }
            if self.segments.len() >= DESCRIPTORS
                && let Some(old) = self.segments.keys().next().copied()
            {
                self.segments.remove(&old);
            }
            self.segments.insert(segment, Segment { file, fingerprint });
            Ok(())
        }
        pub(crate) fn before_append(
            &mut self,
            dir: &Path,
            segment: u64,
            expected_len: u64,
        ) -> Result<Option<AppendCheck>, QueueError> {
            if !self.trusts.contains_key(&segment) {
                return Ok(None);
            }
            self.prepare(dir, segment)?;
            let open = self.segments.get(&segment).ok_or(QueueError::Io)?;
            if open.fingerprint.len != expected_len {
                self.invalidate_segment(segment);
                return Err(QueueError::Io);
            }
            if self.trusts.contains_key(&segment) {
                Ok(Some(AppendCheck {
                    segment,
                    fingerprint: open.fingerprint.clone(),
                }))
            } else {
                Ok(None)
            }
        }
        pub(crate) fn after_append(
            &mut self,
            dir: &Path,
            check: Option<AppendCheck>,
            expected_len: u64,
        ) -> Result<(), QueueError> {
            let Some(check) = check else {
                return Ok(());
            };
            let result = (|| {
                let open = self.segments.get(&check.segment).ok_or(QueueError::Io)?;
                let fingerprint = Fingerprint::new(&storage::private_metadata(
                    &open.file,
                    &storage::segment(dir, check.segment),
                )?)?;
                if !fingerprint.same_file(&check.fingerprint) || fingerprint.len != expected_len {
                    return Err(QueueError::Io);
                }
                if let Some(trust) = self.trusts.get_mut(&check.segment) {
                    if trust.fingerprint != check.fingerprint {
                        return Err(QueueError::Io);
                    }
                    trust.fingerprint = fingerprint.clone();
                }
                if let Some(open) = self.segments.get_mut(&check.segment) {
                    open.fingerprint = fingerprint;
                }
                Ok(())
            })();
            if result.is_err() {
                self.invalidate_segment(check.segment);
                self.segments.remove(&check.segment);
            }
            result
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
                if cached.segment == body.segment
                    && self
                        .trusts
                        .get(&body.segment)
                        .is_some_and(|t| t.fingerprint == open.fingerprint)
                {
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
                        payload: payload.clone(),
                        ticket,
                    },
                );
                self.order.insert(ticket, body.meta.id);
                self.trusts
                    .entry(body.segment)
                    .or_insert_with(|| Trust {
                        fingerprint,
                        bodies: BTreeSet::new(),
                    })
                    .bodies
                    .insert(body.meta.id);
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
        #[cfg(test)]
        pub(crate) fn evict_descriptors(&mut self) {
            self.segments.clear();
        }
    }
}
#[cfg(not(unix))]
mod platform {
    use crate::{QueueError, shared::Body, storage};
    use std::{collections::BTreeSet, path::Path};
    pub(crate) struct AppendCheck;
    #[derive(Default)]
    pub(crate) struct ReadCache;
    impl ReadCache {
        pub(crate) fn clear(&mut self) {}
        pub(crate) fn remove(&mut self, _: u64) {}
        pub(crate) fn before_append(
            &mut self,
            _: &Path,
            _: u64,
            _: u64,
        ) -> Result<Option<AppendCheck>, QueueError> {
            Ok(None)
        }
        pub(crate) fn after_append(
            &mut self,
            _: &Path,
            _: Option<AppendCheck>,
            _: u64,
        ) -> Result<(), QueueError> {
            Ok(())
        }
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
pub(crate) use platform::{AppendCheck, ReadCache};
