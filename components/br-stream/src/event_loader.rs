use std::iter;

use engine_traits::{CF_DEFAULT, CF_WRITE};
use log_wrappers::Value as Redact;
use tikv::storage::{
    mvcc::{DeltaScanner, ScannerBuilder},
    txn::{EntryBatch, TxnEntry, TxnEntryScanner},
    Snapshot,
};
use tikv_util::{box_err, warn};
use txn_types::TimeStamp;

use crate::errors::{Error, Result};
use crate::router::ApplyEvent;
use kvproto::kvrpcpb::ExtraOp;

struct EventLoader<S: Snapshot> {
    scanner: DeltaScanner<S>,
    region_id: u64,
}

impl<S: Snapshot> EventLoader<S> {
    fn load_from(
        snapshot: S,
        from_ts: TimeStamp,
        to_ts: TimeStamp,
        region_id: u64,
    ) -> Result<Self> {
        let scanner = ScannerBuilder::new(snapshot, to_ts)
            .hint_min_ts(Some(from_ts))
            .fill_cache(false)
            .range(None, None)
            .build_delta_scanner(from_ts, ExtraOp::Noop)
            .map_err(|err| {
                Error::Other(box_err!(
                    "failed to create entry scanner from_ts = {}, to_ts = {}, region = {}: {}",
                    from_ts,
                    to_ts,
                    region_id,
                    err
                ))
            })?;

        Ok(Self { scanner, region_id })
    }

    /// scan a batch of events from the snapshot.
    /// note: maybe make something like [`EntryBatch`] for reducing allocation.
    fn scan_batch(&mut self, batch_size: usize) -> Result<Vec<ApplyEvent>> {
        let mut b = EntryBatch::with_capacity(batch_size);
        self.scanner.scan_entries(&mut b)?;
        let mut result = Vec::with_capacity(b.len() * 2);
        for entry in b.drain() {
            match entry {
                TxnEntry::Prewrite {
                    default: (key, _), ..
                } => {
                    warn!("skipping txn entry because it is prewrite."; "key" => Redact::key(&key));
                }
                TxnEntry::Commit { default, write, .. } => {
                    let write =
                        ApplyEvent::from_committed(CF_WRITE, write.0, write.1, self.region_id)?;
                    result.push(write);
                    if !default.0.is_empty() {
                        let default = ApplyEvent::from_committed(
                            CF_DEFAULT,
                            default.0,
                            default.1,
                            self.region_id,
                        )?;
                        result.push(default);
                    }
                }
            }
        }
        Ok(result)
    }
}
