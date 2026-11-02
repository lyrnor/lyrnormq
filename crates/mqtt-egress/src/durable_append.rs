//! Append-only group collection; every other command is a FIFO barrier.
use super::*;

const RESULTS: usize = 256;
const FRAME_RESULTS: usize = 64;
const RAW_BYTES: usize = 8 * 1024 * 1024;
const ESTIMATED_BYTES: usize = 16 * 1024 * 1024;
type Receipt = (Option<Reply<()>>, Option<Admission>);

pub(super) struct AppendBatch {
    groups: Vec<Vec<NewResult>>,
    receipts: Vec<Vec<Receipt>>,
}

// JSON escapes each UTF-8 input byte to at most six bytes. Fixed allowances
// conservatively include IDs, checksums, state fields, delimiters and numbers.
fn estimated_bytes(record: &NewResult) -> usize {
    record
        .payload
        .len()
        .saturating_add(1024)
        .saturating_add(record.stream_id.len().saturating_mul(6))
        .saturating_add(record.client_id.len().saturating_mul(6))
        .saturating_add(record.consumers.iter().fold(0usize, |total, c| {
            total
                .saturating_add(512)
                .saturating_add(c.outbound_id.len().saturating_mul(6))
                .saturating_add(c.delivery_id.len().saturating_mul(6))
        }))
}
impl AppendBatch {
    pub(super) fn gather(
        first: NewResult,
        reply: Option<Reply<()>>,
        pending: Option<Admission>,
        receiver: &mut mpsc::Receiver<Command>,
        carried: &mut Option<Command>,
    ) -> Self {
        let mut raw = first.payload.len();
        let mut estimate = estimated_bytes(&first);
        let mut count = 1;
        let mut groups = vec![vec![first]];
        let mut receipts = vec![vec![(reply, pending)]];
        while count < RESULTS {
            match receiver.try_recv() {
                Ok(Command::Append(record, reply, pending))
                    if raw.saturating_add(record.payload.len()) <= RAW_BYTES
                        && estimate.saturating_add(estimated_bytes(&record)) <= ESTIMATED_BYTES =>
                {
                    raw += record.payload.len();
                    estimate += estimated_bytes(&record);
                    count += 1;
                    if groups
                        .last()
                        .is_some_and(|frame| frame.len() == FRAME_RESULTS)
                    {
                        groups.push(Vec::new());
                        receipts.push(Vec::new());
                    }
                    if let (Some(frame), Some(replies)) = (groups.last_mut(), receipts.last_mut()) {
                        frame.push(record);
                        replies.push((reply, pending));
                    }
                }
                Ok(other) => {
                    *carried = Some(other);
                    break;
                }
                Err(_) => break,
            }
        }
        Self { groups, receipts }
    }

    pub(super) fn commit(
        self,
        journal: &mut Journal,
        view: &Mutex<View>,
        changed: &watch::Sender<u64>,
        stopped: &AtomicBool,
        counters: &Counters,
    ) {
        // Borrowed input remains available for the only safe fallback: Full is a
        // preflight rejection with no appended frames or consumed identifiers.
        let result = journal.append_result_groups(&self.groups);
        match result {
            Err(QueueError::Full) => {
                let mut fatal_error = None;
                for (records, receipts) in self.groups.into_iter().zip(self.receipts) {
                    let result = if let Some(error) = fatal_error {
                        Err(error)
                    } else {
                        checked(journal.append_results(records), stopped, counters).map(|_| ())
                    };
                    if let Err(error) = result
                        && fatal(&error)
                    {
                        fatal_error = Some(error);
                    }
                    refresh(journal, view);
                    finish(receipts, result);
                    changed.send_modify(|epoch| *epoch = epoch.wrapping_add(1));
                }
            }
            result => {
                let result = checked(result, stopped, counters).map(|_| ());
                refresh(journal, view);
                for receipts in self.receipts {
                    finish(receipts, result);
                }
                changed.send_modify(|epoch| *epoch = epoch.wrapping_add(1));
            }
        }
    }
}
fn finish(receipts: Vec<Receipt>, result: Result<(), QueueError>) {
    for (reply, mut admission) in receipts {
        if result.is_ok()
            && let Some(pending) = &mut admission
        {
            pending.committed = true;
        }
        drop(admission);
        if let Some(reply) = reply {
            let _ = reply.send(result);
        }
    }
}
