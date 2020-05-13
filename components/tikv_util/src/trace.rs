use kvproto::span as spanpb;
use minitrace::CollectorRx;

#[repr(u32)]
pub enum TraceEvent {
    #[allow(dead_code)]
    Unknown = 0u32,
    CoprRequest,
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
            _ => unimplemented!("enumeration not exhausted"),
        }
    }
}

impl Into<spanpb::Event> for TraceEvent {
    fn into(self) -> spanpb::Event {
        match self {
            TraceEvent::Unknown => spanpb::Event::Unknown,
            TraceEvent::CoprRequest => spanpb::Event::CoprRequest,
        }
    }
}

pub fn encode_spans(rx: CollectorRx) -> protobuf::RepeatedField<spanpb::Span> {
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
    spans.collect()
}
