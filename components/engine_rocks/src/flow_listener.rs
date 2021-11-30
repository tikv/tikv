// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::hash_set_with_capacity;
use rocksdb::{CompactionJobInfo, EventListener, FlushJobInfo, IngestionInfo};

use std::sync::mpsc::Sender;
use std::sync::Mutex;

pub enum FlowInfo {
    L0(String, u64),
    L0Intra(String, u64),
    Flush(String, u64),
    Compaction(String),
    BeforeUnsafeDestroyRange,
    AfterUnsafeDestroyRange,
}

pub struct FlowListener {
    flow_info_sender: Mutex<Sender<FlowInfo>>,
}

impl FlowListener {
    pub fn new(flow_info_sender: Sender<FlowInfo>) -> Self {
        Self {
            flow_info_sender: Mutex::new(flow_info_sender),
        }
    }
}

impl EventListener for FlowListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let mut total = 0;
        let p = info.table_properties();
        total += p.data_size() + p.index_size() + p.filter_size();
        let _ = self
            .flow_info_sender
            .lock()
            .unwrap()
            .send(FlowInfo::Flush(info.cf_name().to_owned(), total));
    }

    fn on_external_file_ingested(&self, info: &IngestionInfo) {
        // we can regard ingestion in L0 as a flush
        if info.picked_level() == 0 {
            let mut total = 0;
            let p = info.table_properties();
            total += p.data_size() + p.index_size() + p.filter_size();
            let _ = self
                .flow_info_sender
                .lock()
                .unwrap()
                .send(FlowInfo::Flush(info.cf_name().to_owned(), total));
        } else {
            // ingestion may change the pending bytes.
            let _ = self
                .flow_info_sender
                .lock()
                .unwrap()
                .send(FlowInfo::Compaction(info.cf_name().to_owned()));
        }
    }

    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        if info.status().is_err() {
            return;
        }

        if info.base_input_level() == 0 {
            // L0 intra compaction
            if info.output_level() == 0 {
                let mut input_files = hash_set_with_capacity(info.input_file_count());
                let mut output_files = hash_set_with_capacity(info.output_file_count());
                for i in 0..info.input_file_count() {
                    info.input_file_at(i)
                        .to_str()
                        .map(|x| input_files.insert(x.to_owned()));
                }
                for i in 0..info.output_file_count() {
                    info.output_file_at(i)
                        .to_str()
                        .map(|x| output_files.insert(x.to_owned()));
                }
                let mut input = 0;
                let mut output = 0;
                let iter = info.table_properties().into_iter();
                for (file, prop) in iter {
                    if input_files.contains(file) {
                        input += prop.data_size() + prop.index_size() + prop.filter_size();
                    } else if output_files.contains(file) {
                        output += prop.data_size() + prop.index_size() + prop.filter_size();
                    }
                }

                let diff = if output < input { input - output } else { 0 };

                let _ = self
                    .flow_info_sender
                    .lock()
                    .unwrap()
                    .send(FlowInfo::L0Intra(info.cf_name().to_owned(), diff));
            } else {
                let l0_input_file_at_input_level =
                    info.input_file_count() - info.num_input_files_at_output_level();
                let mut files = hash_set_with_capacity(l0_input_file_at_input_level);
                let props = info.table_properties();
                let mut read_bytes = 0;
                for i in 0..l0_input_file_at_input_level {
                    info.input_file_at(i)
                        .to_str()
                        .map(|x| files.insert(x.to_owned()));
                }

                for (file, prop) in props.iter() {
                    if files.contains(file) {
                        read_bytes += prop.data_size() + prop.index_size() + prop.filter_size();
                    }
                }

                let _ = self
                    .flow_info_sender
                    .lock()
                    .unwrap()
                    .send(FlowInfo::L0(info.cf_name().to_owned(), read_bytes));
            }
        }

        let _ = self
            .flow_info_sender
            .lock()
            .unwrap()
            .send(FlowInfo::Compaction(info.cf_name().to_owned()));
    }
}
