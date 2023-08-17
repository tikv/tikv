// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tikv::server::MEM_TRACE_SUM_GAUGE;
use tikv_alloc::{trace::MemoryTrace, Id};
use tikv_util::time::Instant;

#[derive(Default)]
pub struct MemoryTraceManager {
    providers: Vec<Arc<MemoryTrace>>,
}

impl MemoryTraceManager {
    pub fn flush(&mut self, _now: Instant) {
        let raft_router = "raft_router".to_string();
        let apply_router = "apply_router".to_string();
        for provider in &self.providers {
            let provider_name = provider.name();
            let ids = provider.get_children_ids();
            for id in ids {
                let sub_trace = provider.sub_trace(id);
                let sub_trace_name = sub_trace.name();
                if sub_trace_name == raft_router || sub_trace_name == apply_router {
                    let alive = sub_trace.sub_trace(Id::Name("alive"));
                    let leak = sub_trace.sub_trace(Id::Name("leak"));
                    MEM_TRACE_SUM_GAUGE
                        .with_label_values(&[&format!(
                            "{}-{}-alive",
                            provider_name, sub_trace_name
                        )])
                        .set(alive.sum() as i64);

                    MEM_TRACE_SUM_GAUGE
                        .with_label_values(&[&format!("{}-{}-leak", provider_name, sub_trace_name)])
                        .set(leak.sum() as i64);
                } else {
                    MEM_TRACE_SUM_GAUGE
                        .with_label_values(&[&format!("{}-{}", provider_name, sub_trace_name)])
                        .set(sub_trace.sum() as i64);
                }
            }

            MEM_TRACE_SUM_GAUGE
                .with_label_values(&[&provider_name])
                .set(provider.sum() as i64)
        }
    }

    pub fn register_provider(&mut self, provider: Arc<MemoryTrace>) {
        let p = &mut self.providers;
        p.push(provider);
    }
}
