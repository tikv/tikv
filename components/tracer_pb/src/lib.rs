#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

pub use crate::protos::tracer_pb::*;

#[cfg(feature = "protobuf-codec")]
pub fn serialize(spans: impl Iterator<Item = tracer::Span>) -> Vec<u8> {
    use protobuf::{Message, RepeatedField};

    let spans: Vec<_> = spans
        .map(|span| {
            let mut s = self::Span::default();
            s.set_id(span.id as u32);
            if let Some(p) = span.parent {
                s.set_parent_value(p as u32);
            }
            s.set_start(span.elapsed_start.as_nanos() as u64);
            s.set_end(span.elapsed_end.as_nanos() as u64);
            s
        })
        .collect();

    let mut resp = self::TracerResp::default();
    resp.set_spans(RepeatedField::from_slice(&spans));

    resp.write_to_bytes().unwrap()
}

#[cfg(feature = "prost-codec")]
pub fn serialize(spans: impl Iterator<Item = tracer::Span>) -> Vec<u8> {
    use prost::Message;

    let spans: Vec<_> = spans
        .map(|span| {
            let mut s = self::Span::default();
            s.id = span.id as u32;
            s.parent = if let Some(p) = span.parent {
                Some(self::span::Parent::ParentValue(p as u32))
            } else {
                Some(self::span::Parent::ParentNone(true))
            };
            s.start = span.elapsed_start.as_nanos() as u64;
            s.end = span.elapsed_end.as_nanos() as u64;
            s
        })
        .collect();

    let mut resp = self::TracerResp::default();
    resp.spans = spans;

    let mut res = vec![];
    let _ = resp.encode(&mut res);
    res
}
