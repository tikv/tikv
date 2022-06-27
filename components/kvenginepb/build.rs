// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::env;

fn main() {
    let gen = env::var_os("GEN_KVENGINEPB").map_or(false, |s| s == "1");
    if !gen {
        return;
    }
    protobuf_codegen_pure::run(protobuf_codegen_pure::Args {
        out_dir: "src",
        input: &["src/changeset.proto"],
        includes: &["src"],
        customize: protobuf_codegen_pure::Customize {
            ..Default::default()
        },
    })
    .expect("protoc");
}
