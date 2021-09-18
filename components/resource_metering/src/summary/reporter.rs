// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::client::Client;
use crate::reporter::SubReporter;
use crate::summary::SummaryRecord;
use crate::Config;
use collections::HashMap;
use std::sync::Arc;

/// An implementation of [SubReporter] for reporting summary records through [Client].
///
/// The `SummaryReporter` internally aggregates the reported items and upload them
/// to the remote server through the `Client`.
///
/// See [SubReporter] for more relevant designs.
///
/// [SubReporter]: crate::reporter::SubReporter
/// [Client]: crate::client::Client
pub struct SummaryReporter<C> {
    client: C,
    records: HashMap<Vec<u8>, SummaryRecord>,
}

impl<C> SummaryReporter<C> {
    pub fn new(client: C) -> Self {
        Self {
            client,
            records: HashMap::default(),
        }
    }
}

impl<C> SubReporter<Arc<HashMap<Vec<u8>, SummaryRecord>>> for SummaryReporter<C>
where
    C: Client,
{
    fn report(&mut self, _cfg: &Config, v: Arc<HashMap<Vec<u8>, SummaryRecord>>) {
        for (tag, summary) in v.iter() {
            match self.records.get(tag) {
                Some(record) => {
                    record.merge(summary);
                }
                None => {
                    self.records.insert(tag.clone(), summary.clone());
                }
            }
        }
    }

    fn upload(&mut self, cfg: &Config) {
        if self.records.is_empty() {
            return;
        }
        // Whether endpoint exists or not, records should be taken in order to reset.
        let records = std::mem::take(&mut self.records);
        self.client
            .upload_summary_records(&cfg.agent_address, &records);
    }

    fn reset(&mut self) {
        self.records.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;
    use tikv_util::config::ReadableDuration;

    struct MockClient;

    impl Client for MockClient {
        fn upload_summary_records(&mut self, address: &str, v: &HashMap<Vec<u8>, SummaryRecord>) {
            assert_eq!(address, "abc");
            assert_eq!(v.len(), 3);
        }
    }

    #[test]
    fn test_summary_reporter() {
        let cfg = Config {
            enabled: false,
            agent_address: "abc".to_owned(),
            report_agent_interval: ReadableDuration::minutes(1),
            max_resource_groups: 3,
            precision: ReadableDuration::secs(1),
        };
        let mut records1 = HashMap::default();
        records1.insert(
            b"a".to_vec(),
            SummaryRecord {
                r_count: AtomicU64::new(1),
                w_count: AtomicU64::new(2),
            },
        );
        records1.insert(
            b"b".to_vec(),
            SummaryRecord {
                r_count: AtomicU64::new(1),
                w_count: AtomicU64::new(2),
            },
        );
        records1.insert(
            b"c".to_vec(),
            SummaryRecord {
                r_count: AtomicU64::new(1),
                w_count: AtomicU64::new(2),
            },
        );
        let records2 = records1.clone();
        let records3 = records1.clone();
        let mut reporter = SummaryReporter::new(MockClient);
        assert_eq!(reporter.records.len(), 0);
        reporter.report(&cfg, Arc::new(records1));
        assert_eq!(reporter.records.len(), 3);
        reporter.report(&cfg, Arc::new(records2));
        assert_eq!(reporter.records.len(), 3);
        reporter.report(&cfg, Arc::new(records3));
        assert_eq!(reporter.records.len(), 3);
        assert_eq!(
            reporter
                .records
                .get("a".as_bytes())
                .unwrap()
                .r_count
                .load(Relaxed),
            3
        );
        assert_eq!(
            reporter
                .records
                .get("a".as_bytes())
                .unwrap()
                .w_count
                .load(Relaxed),
            6
        );
        assert_eq!(
            reporter
                .records
                .get("b".as_bytes())
                .unwrap()
                .r_count
                .load(Relaxed),
            3
        );
        assert_eq!(
            reporter
                .records
                .get("b".as_bytes())
                .unwrap()
                .w_count
                .load(Relaxed),
            6
        );
        assert_eq!(
            reporter
                .records
                .get("c".as_bytes())
                .unwrap()
                .r_count
                .load(Relaxed),
            3
        );
        assert_eq!(
            reporter
                .records
                .get("c".as_bytes())
                .unwrap()
                .w_count
                .load(Relaxed),
            6
        );
        reporter.upload(&cfg);
        reporter.reset();
        assert!(reporter.records.is_empty())
    }
}
