use kvproto::span as spanpb;
use minitrace::SpanSet;

pub fn encode_spans(span_sets: Vec<SpanSet>) -> impl Iterator<Item = spanpb::SpanSet> {
    span_sets
        .into_iter()
        .map(|span_set| {
            let mut pb_set = spanpb::SpanSet::default();
            pb_set.set_create_time_ns(span_set.create_time_ns);
            pb_set.set_start_time_ns(span_set.start_time_ns);
            pb_set.set_cycles_per_sec(span_set.cycles_per_sec);

            let spans = span_set.spans.into_iter().map(|span| {
                let mut s = spanpb::Span::default();
                s.set_id(span.id);
                s.set_begin_cycles(span.begin_cycles);
                s.set_end_cycles(span.end_cycles);
                s.set_event(span.event);

                #[cfg(feature = "prost-codec")]
                {
                    use minitrace::Link;
                    s.link = Some(spanpb::Link {
                        link: Some(match span.link {
                            Link::Root => spanpb::link::Link::Root(spanpb::Root {}),
                            Link::Parent { id } => {
                                spanpb::link::Link::Parent(spanpb::Parent { id })
                            }
                            Link::Continue { id } => {
                                spanpb::link::Link::Continue(spanpb::Continue { id })
                            }
                        }),
                    });
                }

                #[cfg(feature = "protobuf-codec")]
                {
                    use minitrace::Link;
                    let mut link = spanpb::Link::new();
                    match span.link {
                        Link::Root => link.set_root(spanpb::Root::new()),
                        Link::Parent { id } => {
                            let mut parent = spanpb::Parent::new();
                            parent.set_id(id);
                            link.set_parent(parent);
                        }
                        Link::Continue { id } => {
                            let mut cont = spanpb::Continue::new();
                            cont.set_id(id);
                            link.set_continue(cont);
                        }
                    };
                    s.set_link(link);
                }
                s
            });

            pb_set.set_spans(spans.collect());

            pb_set
        })
        .into_iter()
}
