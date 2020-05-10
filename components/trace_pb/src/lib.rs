#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

pub use crate::protos::trace_pb::*;

pub fn serialize(spans: impl Iterator<Item = minitrace::Span>) -> Vec<u8> {
    #[cfg(feature = "protobuf-codec")]
    use protobuf::Message;
    #[cfg(feature = "prost-codec")]
    use prost::Message;

    let mut resp = self::TracerResp::default();
    resp.set_spans(
        spans
            .map(|span| {
                let mut s = self::Span::default();

                s.set_id(span.id.into());
                s.set_start(span.elapsed_start);
                s.set_end(span.elapsed_end);
                s.set_event_id(span.tag);

                #[cfg(feature = "prost-codec")]
                if let Some(p) = span.parent {
                    s.parent = Some(self::span::Parent::ParentValue(p.into()));
                } else {
                    s.parent = Some(self::span::Parent::ParentNone(true));
                };
                #[cfg(feature = "protobuf-codec")]
                if let Some(p) = span.parent {
                    s.set_parent_value(p.into());
                }
                
                s
            })
            .collect()
    );

    #[cfg(feature = "protobuf-codec")]
    {
        resp.write_to_bytes().unwrap()
    }
    #[cfg(feature = "prost-codec")]
    {
        let mut res = vec![];
        let _ = resp.encode(&mut res);
        res
    }
}
