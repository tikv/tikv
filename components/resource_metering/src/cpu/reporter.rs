// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::client::Client;
use crate::cpu::{CpuRecords, RawCpuRecords};
use crate::reporter::SubReporter;
use crate::Config;
use std::sync::Arc;

/// An implementation of [SubReporter] for reporting cpu statistics through [Client].
///
/// The `CpuReporter` internally aggregates the reported [RawCpuRecords] into
/// [CpuRecords] and upload them to the remote server through the `Client`.
///
/// See [SubReporter] for more relevant designs.
///
/// [SubReporter]: crate::reporter::SubReporter
/// [Client]: crate::client::Client
/// [RawCpuRecords]: crate::cpu::RawCpuRecords
/// [CpuRecords]: crate::cpu::CpuRecords
pub struct CpuReporter<C> {
    client: C,
    records: CpuRecords,
}

impl<C> CpuReporter<C> {
    pub fn new(client: C) -> Self {
        Self {
            client,
            records: CpuRecords::default(),
        }
    }
}

impl<C> SubReporter<Arc<RawCpuRecords>> for CpuReporter<C>
where
    C: Client,
{
    fn report(&mut self, cfg: &Config, v: Arc<RawCpuRecords>) {
        self.records.append(v);
        self.records.keep_top_k(cfg.max_resource_groups);
    }

    fn upload(&mut self, cfg: &Config) {
        if self.records.is_empty() {
            return;
        }
        // Whether endpoint exists or not, records should be taken in order to reset.
        let records = std::mem::take(&mut self.records);
        self.client.upload_cpu_records(&cfg.agent_address, records);
    }

    fn reset(&mut self) {
        self.records.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ResourceMeteringTag, TagInfos};
    use collections::HashMap;
    use std::time::Duration;
    use tikv_util::config::ReadableDuration;

    struct MockClient;

    impl Client for MockClient {
        fn upload_cpu_records(&mut self, address: &str, records: CpuRecords) {
            assert_eq!(address, "abc");
            assert_eq!(records.records.len(), 2);
            assert_eq!(records.others.len(), 1);
        }
    }

    #[test]
    fn test_cpu_reporter() {
        let cfg = Config {
            enabled: false,
            agent_address: "abc".to_owned(),
            report_agent_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2,
            precision: ReadableDuration::secs(1),
        };
        let tag1 = ResourceMeteringTag::from(Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"a".to_vec(),
        }));
        let tag2 = ResourceMeteringTag::from(Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"b".to_vec(),
        }));
        let tag3 = ResourceMeteringTag::from(Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"c".to_vec(),
        }));
        let mut raw_map = HashMap::default();
        raw_map.insert(tag1, 111);
        raw_map.insert(tag2, 222);
        raw_map.insert(tag3, 333);
        let raw = Arc::new(RawCpuRecords {
            begin_unix_time_secs: 1,
            duration: Duration::from_secs(1),
            records: raw_map,
        });
        let mut reporter = CpuReporter::new(MockClient);
        reporter.report(&cfg, raw.clone());
        reporter.report(&cfg, raw.clone());
        reporter.report(&cfg, raw.clone());
        reporter.upload(&cfg);
        reporter.reset();
        assert!(reporter.records.is_empty());
    }
}
