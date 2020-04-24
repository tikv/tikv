#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
extern crate tikv_alloc;

include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

use protobuf::{Message, RepeatedField};
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos() as u64
}

pub fn serialize(spans: impl Iterator<Item = tracer::Span>) -> Vec<u8> {
    let spans: Vec<_> = spans
        .map(|span| {
            let mut s = crate::tracer_pb::Span::default();
            s.set_id(span.id as u32);
            if let Some(p) = span.parent {
                s.set_parent_value(p as u32);
            }
            s.set_start(timestamp(span.start_time));
            s.set_end(timestamp(span.end_time));
            s
        })
        .collect();

    let mut resp = crate::tracer_pb::TracerResp::default();
    resp.set_spans(RepeatedField::from_slice(&spans));

    resp.write_to_bytes().unwrap()
}
