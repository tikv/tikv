// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
