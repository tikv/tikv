#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate tikv;

fuzz_target!(|data: &[u8]| {
    use tikv::util::codec;

    let _ = codec::bytes::encode_bytes(data);
    let _ = codec::bytes::encode_bytes_desc(data);
    let _ = codec::bytes::encoded_compact_len(data);
    let _ = codec::bytes::encoded_bytes_len(data, true);
    let _ = codec::bytes::encoded_bytes_len(data, false);
});
