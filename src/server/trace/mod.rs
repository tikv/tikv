// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::hash_set_with_capacity;
use tikv_util::trace::span::Span;

#[macro_export]
macro_rules! trace_and_fill_resp {
    ($req_name: ident, $config: expr, $req: expr, $resp_ty: ty, $batch_begin_unix_time_ns: expr) => {{
        use tikv_util::trace::CollectArgs;
        use tikv_util::trace::Span;

        let trace_context = $req.mut_context().take_trace_context();
        let enabled = trace_context.get_enabled();

        let (span, collector) = if enabled {
            let (root, collector) = Span::root(stringify!($req_name));
            (root, Some(collector))
        } else {
            (Span::empty(), None)
        };

        let duration_threshold = $config.duration_threshold.0;
        let max_spans_length = $config.max_spans_length;

        (span, move |resp: &mut $resp_ty| {
            if collector.is_none() {
                return;
            }

            let mut spans = collector
                .unwrap()
                .collect_with_args(CollectArgs::default().duration_threshold(duration_threshold));

            crate::server::trace::evict_spans(&mut spans, max_spans_length);

            let trace_detail = resp.mut_meta().mut_trace_detail();
            let span_sets = trace_detail.mut_span_sets();
            let span_set = span_sets.push_default();
            span_set.set_service_name("TiKV".to_owned());
            span_set.set_trace_id(trace_context.get_trace_id());

            let span_id_prefix = rand::random::<u32>();

            let is_batch = $batch_begin_unix_time_ns != 0;
            let batch_span_id = ((span_id_prefix as u64) << 32) | (rand::random::<u32>() as u64);
            let root_parent_id = if is_batch {
                batch_span_id
            } else {
                trace_context.get_parent_span_id()
            };

            let pb_spans = span_set.mut_spans();
            for span in spans {
                let pb_span = pb_spans.push_default();
                pb_span.set_id(((span_id_prefix as u64) << 32) | (span.id as u64));
                pb_span.set_parent_id(if span.parent_id == 0 {
                    root_parent_id
                } else {
                    ((span_id_prefix as u64) << 32) | (span.parent_id as u64)
                });
                pb_span.set_begin_unix_time_ns(span.begin_unix_time_ns);
                pb_span.set_duration_ns(span.duration_ns);
                pb_span.set_event(span.event.to_owned());
                let pb_properties = pb_span.mut_properties();
                for (k, v) in span.properties {
                    let pb_property = pb_properties.push_default();
                    pb_property.set_key(k.to_owned());
                    pb_property.set_value(v);
                }
            }

            if $batch_begin_unix_time_ns == 0 {
                return;
            }

            let pb_span = pb_spans.push_default();
            pb_span.set_id(batch_span_id);
            pb_span.set_parent_id(trace_context.get_parent_span_id());
            pb_span.set_begin_unix_time_ns($batch_begin_unix_time_ns);
            pb_span.set_event(concat!("_batch_command_", stringify!($req_name)).to_owned());
        })
    }};
}

pub fn evict_spans(spans: &mut Vec<Span>, max_length: usize) {
    if spans.len() > max_length {
        spans.sort_by_key(|s| s.begin_unix_time_ns);
        spans.truncate(max_length);
    }

    // Repair spans have unresolved parent id
    let mut root_id = spans[0].id;
    let mut id_set = hash_set_with_capacity(spans.len());
    for span in spans.iter_mut() {
        id_set.insert(span.id);
        if span.parent_id == 0 {
            root_id = span.id;
        }
    }

    for span in spans.iter_mut() {
        if span.parent_id != 0 && !id_set.contains(&span.parent_id) {
            span.parent_id = root_id;
        }
    }
}
