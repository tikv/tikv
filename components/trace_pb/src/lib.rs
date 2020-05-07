#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

pub use crate::protos::trace_pb::*;

#[cfg(feature = "protobuf-codec")]
pub fn serialize(spans: impl Iterator<Item = minitrace::Span>) -> Vec<u8> {
    use protobuf::{Message, RepeatedField};

    let spans: Vec<_> = spans
        .map(|span| {
            let mut s = self::Span::default();
            s.set_id(span.id.into());
            if let Some(p) = span.parent {
                s.set_parent_value(p.into());
            }
            s.set_start(span.elapsed_start);
            s.set_end(span.elapsed_end);
            s
        })
        .collect();

    let mut resp = self::TracerResp::default();
    resp.set_spans(RepeatedField::from_slice(&spans));

    resp.write_to_bytes().unwrap()
}

#[cfg(feature = "prost-codec")]
pub fn serialize(spans: impl Iterator<Item = minitrace::Span>) -> Vec<u8> {
    use prost::Message;

    let spans: Vec<_> = spans
        .map(|span| {
            let mut s = self::Span::default();
            s.id = span.id.into();
            s.parent = if let Some(p) = span.parent {
                Some(self::span::Parent::ParentValue(p.into()))
            } else {
                Some(self::span::Parent::ParentNone(true))
            };
            s.start = span.elapsed_start;
            s.end = span.elapsed_end;
            s
        })
        .collect();

    let mut resp = self::TracerResp::default();
    resp.spans = spans;

    let mut res = vec![];
    let _ = resp.encode(&mut res);
    res
}
