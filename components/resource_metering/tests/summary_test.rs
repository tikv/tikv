// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use arc_swap::ArcSwap;
use collections::HashMap;
use kvproto::kvrpcpb::Context;
use online_config::{ConfigChange, ConfigManager, ConfigValue};
use resource_metering::error::Result;
use resource_metering::{
    Config, DataSink, Record, RecorderBuilder, Records, Reporter, SummaryRecorder,
};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tikv_util::config::ReadableDuration;
use tikv_util::worker::LazyWorker;

const PRECISION_MS: u64 = 1000;
const REPORT_INTERVAL_MS: u64 = 3000;

#[derive(Default, Clone)]
struct MockClient {
    data: Arc<Mutex<HashMap<Vec<u8>, Record>>>,
}

impl DataSink for MockClient {
    fn try_send(&mut self, records: Records) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        records.records.iter().for_each(|(k, v)| {
            data.insert(k.clone(), v.clone());
        });
        Ok(())
    }
}

impl MockClient {
    fn get(&self, k: &[u8]) -> Option<Record> {
        self.data.lock().unwrap().get(k).cloned()
    }

    fn clear(&self) {
        self.data.lock().unwrap().clear();
    }
}

#[test]
fn test_summary() {
    let client = MockClient::default();

    let mut cfg = Config::default();
    cfg.receiver_address = "127.0.0.1:12345".to_owned();
    cfg.report_receiver_interval = ReadableDuration::millis(REPORT_INTERVAL_MS);

    let (rh, crh, tf) = RecorderBuilder::default()
        .enable(cfg.enabled)
        .precision_ms(Arc::new(AtomicU64::new(PRECISION_MS)))
        .add_sub_recorder(Box::new(SummaryRecorder::new(cfg.enabled)))
        .spawn()
        .expect("failed to create resource metering thread");
    let mut worker = LazyWorker::new("test-worker");
    worker.start_with_timer(Reporter::new(
        client.clone(),
        cfg.clone(),
        crh,
        worker.scheduler(),
    ));
    let address = Arc::new(ArcSwap::new(Arc::new(cfg.receiver_address.clone())));
    let mut cfg_manager =
        resource_metering::ConfigManager::new(cfg, worker.scheduler(), rh, address);

    /* At this point we are ready for everything except turning on the switch. */

    // expect no data
    {
        let tf = tf.clone();
        let client = client.clone();
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
            assert!(client.get(&b"TAG-1".to_vec()).is_none());
            client.clear();
        })
        .join()
        .unwrap();
    }

    // turn on
    let mut change = ConfigChange::new();
    change.insert("enabled".to_owned(), ConfigValue::Bool(true));
    cfg_manager.dispatch(change).unwrap();

    // expect can get data
    {
        let tf = tf.clone();
        let client = client.clone();
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
            let r = client.get(&b"TAG-1".to_vec()).unwrap();
            assert_eq!(r.read_keys_list.iter().sum::<u32>(), 123);
            assert_eq!(r.write_keys_list.iter().sum::<u32>(), 456);
            client.clear();
        })
        .join()
        .unwrap();
    }

    // turn off
    let mut change = ConfigChange::new();
    change.insert("enabled".to_owned(), ConfigValue::Bool(false));
    cfg_manager.dispatch(change).unwrap();

    // expect no data
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
        assert!(client.get(&b"TAG-1".to_vec()).is_none());
        client.clear();
    })
    .join()
    .unwrap();

    // stop worker
    worker.stop();
}
