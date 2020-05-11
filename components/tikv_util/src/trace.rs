use kvproto::span as spanpb;
use minitrace::CollectorRx;

#[repr(u32)]
pub enum Trace {
    #[allow(dead_code)]
    Unknown = 0u32,
    Copr,
}

impl Into<u32> for Trace {
    fn into(self) -> u32 {
        self as u32
    }
}

pub fn encode_spans(rx: CollectorRx) -> protobuf::RepeatedField<spanpb::Span> {
    let finished_spans = rx.collect();
    let spans = finished_spans.into_iter().map(|span| {
        let mut s = spanpb::Span::default();

        s.set_id(span.id.into());
        s.set_start(span.elapsed_start);
        s.set_end(span.elapsed_end);
        s.set_event_id(span.tag);

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
