// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tikv::server::MEM_TRACE_SUM_GAUGE;
use tikv_alloc::trace::MemoryTrace;
use tikv_util::time::Instant;

#[derive(Default)]
pub struct MemoryTraceManager {
    providers: Vec<Arc<MemoryTrace>>,
}

impl MemoryTraceManager {
    pub fn flush(&mut self, _now: Instant) {
        for provider in &self.providers {
            let provider_name = provider.name();
            let ids = provider.get_children_ids();
            for id in ids {
                let sub_trace = provider.sub_trace(id);
                let sub_trace_name = sub_trace.name();
                let leaf_ids = sub_trace.get_children_ids();
                if leaf_ids.is_empty() {
                    MEM_TRACE_SUM_GAUGE
                        .with_label_values(&[&format!("{}-{}", provider_name, sub_trace_name)])
                        .set(sub_trace.sum() as i64);
                } else {
                    for leaf_id in leaf_ids {
                        let leaf = sub_trace.sub_trace(leaf_id);
                        MEM_TRACE_SUM_GAUGE
                            .with_label_values(&[&format!(
                                "{}-{}-{}",
                                provider_name,
                                sub_trace_name,
                                leaf.name(),
                            )])
                            .set(leaf.sum() as i64);
                    }
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
