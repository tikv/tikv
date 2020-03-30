use rustracing::span::FinishedSpan;
use rustracing_jaeger::span::SpanContextState;
use rustracing_jaeger::thrift::jaeger::{Batch, Process};
use rustracing_jaeger::{Error, ErrorKind};
use thrift_codec::message::Message;
use thrift_codec::BinaryEncode;

pub fn from_thrift_error(f: thrift_codec::Error) -> Error {
    match *f.kind() {
        thrift_codec::ErrorKind::InvalidInput => ErrorKind::InvalidInput.into(),
        thrift_codec::ErrorKind::Other => ErrorKind::Other.into(),
    }
}

pub fn encode_spans_in_jaeger_binary(
    spans: &[FinishedSpan<SpanContextState>],
) -> Result<Vec<u8>, Error> {
    let batch = Batch {
        process: Process {
            service_name: "tikv".to_string(),
            tags: Vec::new(),
        },
        spans: spans.iter().map(From::from).collect(),
    };
    let message = Message::from(rustracing_jaeger::thrift::agent::EmitBatchNotification { batch });
    let mut bytes = Vec::new();
    message
        .binary_encode(&mut bytes)
        .map_err(from_thrift_error)?;
    Ok(bytes)
}
