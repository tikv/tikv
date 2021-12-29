// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use collections::HashMap;
use grpcio::Environment;
use kvproto::kvrpcpb::Context;
use kvproto::resource_usage_agent::ResourceUsageRecord;
use online_config::{ConfigChange, ConfigManager, ConfigValue};
use resource_metering::error::Result;
use resource_metering::{
    init_reporter, init_single_target, Config, DataSink, RecorderBuilder, SummaryRecorder,
};
use tikv_util::config::ReadableDuration;

const PRECISION_MS: u64 = 1000;
const REPORT_INTERVAL_MS: u64 = 3000;

#[derive(Default, Clone)]
struct MockClient {
    data: Arc<Mutex<HashMap<Vec<u8>, ResourceUsageRecord>>>,
}

impl DataSink for MockClient {
    fn try_send(&mut self, records: Arc<Vec<ResourceUsageRecord>>) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        records.iter().for_each(|r| {
            data.insert(r.resource_group_tag.clone(), r.clone());
        });
        Ok(())
    }
}

impl MockClient {
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
        ..Default::default()
    };

    let (recorder_handle, collector_reg_handle, resource_tag_factory) = RecorderBuilder::default()
        .enable(cfg.enabled)
        .precision_ms(Arc::new(AtomicU64::new(PRECISION_MS)))
        .add_sub_recorder(Box::new(SummaryRecorder::new(cfg.enabled)))
        .spawn()
        .expect("failed to create resource metering thread");
    let (config_notifier, data_sink_reg_handle, mut reporter_worker) =
        init_reporter(cfg.clone(), collector_reg_handle);
    let (address_notifier, mut single_target_worker) = init_single_target(
        cfg.receiver_address.clone(),
        Arc::new(Environment::new(2)),
        data_sink_reg_handle.clone(),
    );
    let mut cfg_manager = resource_metering::ConfigManager::new(
        cfg,
        recorder_handle,
        config_notifier,
        address_notifier,
    );

    let client = MockClient::default();
    let _reg_guard = data_sink_reg_handle.register(Box::new(client.clone()));

    /* At this point we are ready for everything except turning on the switch. */

    // expect no data
    {
        let tf = resource_tag_factory.clone();
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
        let tf = resource_tag_factory.clone();
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
            assert_eq!(r.get_record_list_read_keys().iter().sum::<u32>(), 123);
            assert_eq!(r.get_record_list_write_keys().iter().sum::<u32>(), 456);
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
            let tag = resource_tag_factory.new_tag(&ctx);
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
    reporter_worker.stop();
    single_target_worker.stop();
}
