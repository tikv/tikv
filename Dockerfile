FROM pingcap/rust as builder

WORKDIR /tikv

# Install Rust
COPY rust-toolchain ./
RUN rustup default nightly-2019-04-25

# Install dependencies at first
COPY Cargo.toml Cargo.lock ./

# Remove fuzz and test workspace, remove profiler feature
RUN sed -i '/fuzz/d' Cargo.toml && \
    sed -i '/test\_/d' Cargo.toml && \
    sed -i '/profiler/d' Cargo.toml

# Add components Cargo files
# Notice: every time we add a new component, we must regenerate the dockerfile
COPY ./components/codec/Cargo.toml ./components/codec/Cargo.toml
COPY ./components/cop_codegen/Cargo.toml ./components/cop_codegen/Cargo.toml
COPY ./components/cop_datatype/Cargo.toml ./components/cop_datatype/Cargo.toml
COPY ./components/engine/Cargo.toml ./components/engine/Cargo.toml
COPY ./components/log_wrappers/Cargo.toml ./components/log_wrappers/Cargo.toml
COPY ./components/match_template/Cargo.toml ./components/match_template/Cargo.toml
COPY ./components/panic_hook/Cargo.toml ./components/panic_hook/Cargo.toml
COPY ./components/tikv_alloc/Cargo.toml ./components/tikv_alloc/Cargo.toml
COPY ./components/tikv_util/Cargo.toml ./components/tikv_util/Cargo.toml
COPY ./components/tipb_helper/Cargo.toml ./components/tipb_helper/Cargo.toml

# Create dummy files, build the dependencies
# then remove TiKV fingerprint for following rebuild
RUN mkdir -p ./src/bin && \
    echo 'fn main() {}' > ./src/bin/tikv-ctl.rs && \
    echo 'fn main() {}' > ./src/bin/tikv-server.rs && \
    echo 'fn main() {}' > ./src/bin/tikv-importer.rs && \
    echo '' > ./src/lib.rs && \
    mkdir ./components/codec/src && echo '' > ./components/codec/src/lib.rs && \
    mkdir ./components/cop_codegen/src && echo '' > ./components/cop_codegen/src/lib.rs && \
    mkdir ./components/cop_datatype/src && echo '' > ./components/cop_datatype/src/lib.rs && \
    mkdir ./components/engine/src && echo '' > ./components/engine/src/lib.rs && \
    mkdir ./components/log_wrappers/src && echo '' > ./components/log_wrappers/src/lib.rs && \
    mkdir ./components/match_template/src && echo '' > ./components/match_template/src/lib.rs && \
    mkdir ./components/panic_hook/src && echo '' > ./components/panic_hook/src/lib.rs && \
    mkdir ./components/tikv_alloc/src && echo '' > ./components/tikv_alloc/src/lib.rs && \
    mkdir ./components/tikv_util/src && echo '' > ./components/tikv_util/src/lib.rs && \
    mkdir ./components/tipb_helper/src && echo '' > ./components/tipb_helper/src/lib.rs && \
    cargo build --no-default-features --release --features "jemalloc portable sse no-fail" && \
    rm -rf ./target/release/.fingerprint/codec-* && \
    rm -rf ./target/release/.fingerprint/cop_codegen-* && \
    rm -rf ./target/release/.fingerprint/cop_datatype-* && \
    rm -rf ./target/release/.fingerprint/engine-* && \
    rm -rf ./target/release/.fingerprint/log_wrappers-* && \
    rm -rf ./target/release/.fingerprint/match_template-* && \
    rm -rf ./target/release/.fingerprint/panic_hook-* && \
    rm -rf ./target/release/.fingerprint/tikv_alloc-* && \
    rm -rf ./target/release/.fingerprint/tikv_util-* && \
    rm -rf ./target/release/.fingerprint/tipb_helper-* && \
    rm -rf ./target/release/.fingerprint/tikv-*

# Build real binaries now
COPY ./src ./src
COPY ./components ./components

RUN make build

# Strip debug info to reduce the docker size, may strip later?
# RUN strip --strip-debug /tikv/target/release/tikv-server && \
#     strip --strip-debug /tikv/target/release/tikv-ctl

FROM pingcap/alpine-glibc
COPY --from=builder /tikv/target/release/tikv-server /tikv-server
COPY --from=builder /tikv/target/release/tikv-ctl /tikv-ctl

EXPOSE 20160 20180

ENTRYPOINT ["/tikv-server"]
