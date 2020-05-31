use kvproto::span as spanpb;
use minitrace::Collector;

#[repr(u32)]
pub enum TraceEvent {
    #[allow(dead_code)]
    Unknown = 0u32,
    CoprRequest = 1u32,
    Scheduled = 2u32,
    Snapshot = 3u32,
    HandleRequest = 4u32,
    HandleUnaryFixture = 5u32,
    HandleChecksum = 6u32,
    HandleDag = 7u32,
    HandleBatchDag = 8u32,
    HandleAnalyze = 9u32,
    HandleCached = 10u32,
}

impl Into<u32> for TraceEvent {
    fn into(self) -> u32 {
        self as u32
    }
}

impl From<u32> for TraceEvent {
    fn from(x: u32) -> Self {
        match x {
            _ if x == TraceEvent::Unknown as u32 => TraceEvent::Unknown,
            _ if x == TraceEvent::CoprRequest as u32 => TraceEvent::CoprRequest,
            _ if x == TraceEvent::Scheduled as u32 => TraceEvent::Scheduled,
            _ if x == TraceEvent::Snapshot as u32 => TraceEvent::Snapshot,
            _ if x == TraceEvent::HandleRequest as u32 => TraceEvent::HandleRequest,
            _ if x == TraceEvent::HandleUnaryFixture as u32 => TraceEvent::HandleUnaryFixture,
            _ if x == TraceEvent::HandleChecksum as u32 => TraceEvent::HandleChecksum,
            _ if x == TraceEvent::HandleDag as u32 => TraceEvent::HandleDag,
            _ if x == TraceEvent::HandleBatchDag as u32 => TraceEvent::HandleBatchDag,
            _ if x == TraceEvent::HandleAnalyze as u32 => TraceEvent::HandleAnalyze,
            _ if x == TraceEvent::HandleCached as u32 => TraceEvent::HandleCached,
            _ => unimplemented!("enumeration not exhausted"),
        }
    }
}

impl Into<spanpb::Event> for TraceEvent {
    fn into(self) -> spanpb::Event {
        match self {
            TraceEvent::Unknown => spanpb::Event::Unknown,
            TraceEvent::CoprRequest => spanpb::Event::CoprRequest,
            TraceEvent::Scheduled => spanpb::Event::Scheduled,
            TraceEvent::Snapshot => spanpb::Event::Snapshot,
            TraceEvent::HandleRequest => spanpb::Event::HandleRequest,
            TraceEvent::HandleUnaryFixture => spanpb::Event::HandleUnaryFixture,
            TraceEvent::HandleChecksum => spanpb::Event::HandleChecksum,
            TraceEvent::HandleDag => spanpb::Event::HandleDag,
            TraceEvent::HandleBatchDag => spanpb::Event::HandleBatchDag,
            TraceEvent::HandleAnalyze => spanpb::Event::HandleAnalyze,
            TraceEvent::HandleCached => spanpb::Event::HandleCached,
        }
    }
}

pub fn encode_spans(rx: Collector) -> impl Iterator<Item = spanpb::SpanSet> {
    /*
    let finished_spans = rx.collect();
    let spans = finished_spans.into_iter().map(|span| {
        let mut s = spanpb::Span::default();

        s.set_id(span.id.into());
        s.set_start(span.elapsed_start);
        s.set_end(span.elapsed_end);
        s.set_event(TraceEvent::from(span.tag).into());

        #[cfg(feature = "prost-codec")]
        if let Some(p) = span.parent {
            s.parent = Some(spanpb::Parent::ParentValue(p.into()));
        } else {
            s.parent = Some(spanpb::Parent::ParentNone(true));
        };
        #[cfg(feature = "protobuf-codec")]
        if let Some(p) = span.parent {
            s.set_parent_value(p.into());
        }
        s
    });
    spans.into_iter()
     */

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
                s.set_event(TraceEvent::from(span.event).into());

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
