use kvproto::span as spanpb;
use minitrace::Collector;

pub fn encode_spans(rx: Collector) -> impl Iterator<Item = spanpb::SpanSet> {
    let span_sets = rx.collect();
    span_sets
        .into_iter()
        .map(|span_set| {
            let mut pb_set = spanpb::SpanSet::default();
            pb_set.set_start_time_ns(span_set.start_time_ns);
            pb_set.set_cycles_per_sec(span_set.cycles_per_sec);

            let spans = span_set.spans.into_iter().map(|span| {
                let mut s = spanpb::Span::default();
                s.set_id(span.id);
                s.set_begin_cycles(span.begin_cycles);
                s.set_end_cycles(span.end_cycles);
                s.set_event(span.event);

                #[cfg(feature = "prost-codec")]
                use minitrace::Link;

                #[cfg(feature = "prost-codec")]
                match span.link {
                    Link::Root => {
                        s.link = spanpb::Link::Root;
                    }
                    Link::Parent(id) => {
                        s.link = spanpb::Link::Parent(id);
                    }
                    Link::Continue(id) => {
                        s.link = spanpb::Link::Continue(id);
                    }
                }

                #[cfg(feature = "protobuf-codec")]
                s.set_link(span.link.into());

                s
            });

            pb_set.set_spans(spans.collect());

            pb_set
        })
        .into_iter()
}
