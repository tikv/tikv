// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use tikv_util::{error, sys::thread::Pid};

use crate::{CpuType, Metric, Resource, ResourceEvent, Scope, Severity};

pub struct ThreadCollector {
    process_id: Pid,
    thread_ids: Vec<Pid>,
}

impl ThreadCollector {
    pub fn new(thread_prefix: String) -> Self {
        let process_id = tikv_util::sys::thread::process_id();
        let mut thread_ids = vec![];
        let thread_name_map = tikv_util::sys::thread::THREAD_NAME_HASHMAP.lock().unwrap();
        for (k, v) in thread_name_map.iter() {
            if v.starts_with(&thread_prefix) {
                thread_ids.push(*k);
            }
        }
        drop(thread_name_map);
        tikv_util::info!(
            "build thread collector for {}, thread count: {}",
            &thread_prefix,
            thread_ids.len()
        );
        Self {
            process_id,
            thread_ids,
        }
    }

    pub(crate) fn collect_cpu_time(&self) -> f64 {
        let mut cpu_time = 0.0;
        for tid in &self.thread_ids {
            if let Ok(stat) = tikv_util::sys::thread::thread_stat(self.process_id, *tid) {
                cpu_time += stat.total_cpu_time();
            }
        }
        cpu_time
    }
}

pub struct DiskCollector {
    dir: String,
}

impl DiskCollector {
    pub fn new(dir: String) -> Self {
        Self { dir }
    }

    pub(crate) fn collect_used_bytes(&self) -> u64 {
        if self.dir.is_empty() {
            return 0;
        }
        let fs_stats = match fs2::statvfs(self.dir.as_str()) {
            Ok(stats) => stats,
            Err(e) => {
                error!("failed to get fs stats for {}, err {:?}", self.dir, e);
                return 0;
            }
        };
        fs_stats.total_space() - fs_stats.available_space()
    }
}

pub struct ResourceCollector {
    cpu: HashMap<CpuType, ThreadCollector>,
    disk: DiskCollector,
    test: bool,
}

impl ResourceCollector {
    pub fn new(dir: String) -> Self {
        let mut cpu = HashMap::new();
        let thread_prefixes = [
            (CpuType::RaftWorker, "raftstore-".to_string()),
            (CpuType::RaftIo, "store-writer-".to_string()),
            (CpuType::Apply, "apply-".to_string()),
            (CpuType::StatusServer, "status-server-".to_string()),
            (CpuType::PdWorker, "pd-worker-".to_string()),
            (CpuType::GrpcServer, "grpc-server-".to_string()),
            (CpuType::UnifiedReadPool, "unified-read-".to_string()),
            (CpuType::TxnScheduler, "sched-".to_string()),
        ];
        for (cpu_type, thread_prefix) in thread_prefixes {
            let raft_worker = ThreadCollector::new(thread_prefix);
            cpu.insert(cpu_type, raft_worker);
        }
        let disk = DiskCollector::new(dir);
        Self {
            cpu,
            disk,
            test: false,
        }
    }

    pub(crate) fn for_test(&mut self, test: bool) {
        self.test = test;
    }

    pub fn collect_cpu_events(&self) -> Option<Vec<ResourceEvent>> {
        if self.test {
            return None;
        }
        let mut cpu_events = Vec::with_capacity(self.cpu.len());
        for (&cpu_type, thread_collector) in &self.cpu {
            let cpu_time = thread_collector.collect_cpu_time();
            cpu_events.push(ResourceEvent::CollectMetric(Metric {
                resource: Resource::Cpu { cpu_type, cpu_time },
                scope: Scope::Global,
                severity: Severity::Normal,
            }))
        }
        Some(cpu_events)
    }

    pub fn collect_disk_events(&self) -> Option<ResourceEvent> {
        if self.test {
            return None;
        }
        let used_bytes = self.disk.collect_used_bytes();
        let event = ResourceEvent::CollectMetric(Metric {
            resource: Resource::Disk { used_bytes },
            scope: Scope::Global,
            severity: Severity::Normal,
        });
        Some(event)
    }
}
