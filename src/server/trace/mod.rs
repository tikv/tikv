// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod reporter;

pub use reporter::*;

macro_rules! trace_and_report {
    ($req_name: ident, $reporter: expr, $req: expr, $resp_ty: ty) => {{
        let trace_context = $req.mut_context().take_trace_context();
        let enable = trace_context.get_enable();

        let (span, collector) = if enable {
            let (root, collector) = Span::root(stringify!($req_name));
            (root, Some(collector))
        } else {
            (Span::empty(), None)
        };

        let reporter = $reporter.clone();

        (span, move |resp: &mut $resp_ty| {
            if !enable {
                return;
            }

            if let Some(spans) = reporter.collect(&trace_context, collector) {
                let trace_detail = resp.mut_meta().mut_trace_detail();
                let span_sets = trace_detail.mut_span_sets();
                let span_set = span_sets.push_default();
                span_set.set_node_type(kvproto::kvrpcpb::TraceDetailNodeType::TiKv);
                span_set.set_root_parent_span_id(trace_context.get_root_parent_span_id());
                span_set.set_trace_id(trace_context.get_trace_id());
                span_set.set_span_id_prefix(trace_context.get_span_id_prefix());

                let pb_spans = span_set.mut_spans();
                for span in spans {
                    let pb_span = pb_spans.push_default();
                    pb_span.set_id(span.id);
                    pb_span.set_parent_id(span.parent_id);
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
            }
        })
    }};
}
