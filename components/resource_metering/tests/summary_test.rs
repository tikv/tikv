// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use collections::HashMap;
use kvproto::{kvrpcpb::Context, resource_usage_agent::ResourceUsageRecord};
use resource_metering::{error::Result, init_recorder, init_reporter, Config, DataSink};
use tikv_util::config::ReadableDuration;

const PRECISION_MS: u64 = 1000;
const REPORT_INTERVAL_MS: u64 = 3000;

#[derive(Default, Clone)]
struct MockDataSink {
    data: Arc<Mutex<HashMap<Vec<u8>, ResourceUsageRecord>>>,
}

impl DataSink for MockDataSink {
    fn try_send(&mut self, records: Arc<Vec<ResourceUsageRecord>>) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        records.iter().for_each(|r| {
            data.insert(r.get_record().get_resource_group_tag().to_vec(), r.clone());
        });
        Ok(())
    }
}

impl MockDataSink {
    fn get(&self, k: &[u8]) -> Option<ResourceUsageRecord> {
        self.data.lock().unwrap().get(k).cloned()
    }

    fn clear(&self) {
        self.data.lock().unwrap().clear();
    }
}

#[test]
fn test_summary() {
    let cfg = Config {
        report_receiver_interval: ReadableDuration::millis(REPORT_INTERVAL_MS),
        precision: ReadableDuration::millis(PRECISION_MS),
        ..Default::default()
    };

    let (_, collector_reg_handle, resource_tag_factory, mut recorder_worker) =
        init_recorder(cfg.precision.as_millis());
    let (_, data_sink_reg_handle, mut reporter_worker) = init_reporter(cfg, collector_reg_handle);

    let data_sink = MockDataSink::default();

    /* At this point we are ready for everything except turning on the switch. */

    // expect no data
    {
        let tf = resource_tag_factory.clone();
        let data_sink = data_sink.clone();
        thread::spawn(move || {
            {
                let mut ctx = Context::default();
                ctx.set_resource_group_tag(b"TAG-1".to_vec());
                let tag = tf.new_tag(&ctx);
                let _g = tag.attach();
                resource_metering::record_read_keys(123);
                resource_metering::record_write_keys(456);
            }
            thread::sleep(Duration::from_millis(REPORT_INTERVAL_MS + 500)); // wait report
            assert!(data_sink.get(b"TAG-1").is_none());
            data_sink.clear();
        })
        .join()
        .unwrap();
    }

    // turn on
    let reg_guard = data_sink_reg_handle.register(Box::new(data_sink.clone()));

    // expect can get data
    {
        let tf = resource_tag_factory.clone();
        let data_sink = data_sink.clone();
        thread::spawn(move || {
            {
                let mut ctx = Context::default();
                ctx.set_resource_group_tag(b"TAG-1".to_vec());
                let tag = tf.new_tag(&ctx);
                let _g = tag.attach();
                thread::sleep(Duration::from_millis(PRECISION_MS * 2)); // wait config apply
                resource_metering::record_read_keys(123);
                resource_metering::record_write_keys(456);
            }
            thread::sleep(Duration::from_millis(REPORT_INTERVAL_MS + 500)); // wait report

            let r = data_sink.get(b"TAG-1").unwrap();
            assert_eq!(
                r.get_record()
                    .get_items()
                    .iter()
                    .map(|item| item.read_keys)
                    .sum::<u32>(),
                123
            );
            assert_eq!(
                r.get_record()
                    .get_items()
                    .iter()
                    .map(|item| item.write_keys)
                    .sum::<u32>(),
                456
            );
            data_sink.clear();
        })
        .join()
        .unwrap();
    }

    // turn off
    drop(reg_guard);

    // expect no data
    thread::spawn(move || {
        {
            let mut ctx = Context::default();
            ctx.set_resource_group_tag(b"TAG-1".to_vec());
            let tag = resource_tag_factory.new_tag(&ctx);
            let _g = tag.attach();
            thread::sleep(Duration::from_millis(PRECISION_MS * 2)); // wait config apply
            resource_metering::record_read_keys(123);
            resource_metering::record_write_keys(456);
        }
        thread::sleep(Duration::from_millis(REPORT_INTERVAL_MS + 500)); // wait report
        assert!(data_sink.get(b"TAG-1").is_none());
        data_sink.clear();
    })
    .join()
    .unwrap();

    // stop worker
    recorder_worker.stop();
    reporter_worker.stop();
}
