fn main() {
    println!(
        "cargo:rerun-if-changed={}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "src/engine_store_ffi/interfaces.rs"
    );
    println!(
        "cargo:rerun-if-changed={}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "../../raftstore-proxy/ffi/src/RaftStoreProxyFFI"
    );
    gen_proxy_ffi_lib::gen_ffi_code();
}
