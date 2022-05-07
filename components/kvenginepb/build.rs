// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

fn main() {
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
