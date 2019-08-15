# This Docker image contains minimal binaries for:
#
# * `tikv-server`
# * `tikv-ctl`
# * `tikv-importer`
#
# All TiKV tools are built in **release** mode.
FROM tikv/rust-toolchain as toolchain

ENV ROCKSDB_SYS_SSE 1

# Install Rust
COPY rust-toolchain ./

# Install dependencies at first
COPY Cargo.toml Cargo.lock ./
COPY Makefile Makefile
COPY scripts scripts

# Remove fuzz and test workspace, remove profiler feature
RUN sed -i '/fuzz/d' Cargo.toml && \
    sed -i '/test\_/d' Cargo.toml && \
    sed -i '/profiler/d' Cargo.toml

# Use Makefile to build
COPY Makefile ./

# For cargo
COPY scripts/run-cargo.sh ./scripts/run-cargo.sh
COPY etc/cargo.config.dist ./etc/cargo.config.dist
# Add components Cargo files
# Notice: every time we add a new component, we must regenerate the dockerfile
COPY ./components/codec/Cargo.toml ./components/codec/Cargo.toml
COPY ./components/engine/Cargo.toml ./components/engine/Cargo.toml
COPY ./components/log_wrappers/Cargo.toml ./components/log_wrappers/Cargo.toml
COPY ./components/match_template/Cargo.toml ./components/match_template/Cargo.toml
COPY ./components/panic_hook/Cargo.toml ./components/panic_hook/Cargo.toml
COPY ./components/tidb_query/Cargo.toml ./components/tidb_query/Cargo.toml
COPY ./components/tidb_query_codegen/Cargo.toml ./components/tidb_query_codegen/Cargo.toml
COPY ./components/tidb_query_datatype/Cargo.toml ./components/tidb_query_datatype/Cargo.toml
COPY ./components/tikv_alloc/Cargo.toml ./components/tikv_alloc/Cargo.toml
COPY ./components/tikv_util/Cargo.toml ./components/tikv_util/Cargo.toml
COPY ./components/tipb_helper/Cargo.toml ./components/tipb_helper/Cargo.toml

# Create dummy files, build the dependencies
# then remove TiKV fingerprint for following rebuild
RUN mkdir -p ./src/bin && \
    echo 'fn main() {}' > ./src/bin/tikv-ctl.rs && \
    echo 'fn main() {}' > ./src/bin/tikv-server.rs && \
    echo '' > ./src/lib.rs && \
    mkdir ./components/codec/src && echo '' > ./components/codec/src/lib.rs && \
    mkdir ./components/engine/src && echo '' > ./components/engine/src/lib.rs && \
    mkdir ./components/log_wrappers/src && echo '' > ./components/log_wrappers/src/lib.rs && \
    mkdir ./components/match_template/src && echo '' > ./components/match_template/src/lib.rs && \
    mkdir ./components/panic_hook/src && echo '' > ./components/panic_hook/src/lib.rs && \
    mkdir ./components/tidb_query/src && echo '' > ./components/tidb_query/src/lib.rs && \
    mkdir ./components/tidb_query_codegen/src && echo '' > ./components/tidb_query_codegen/src/lib.rs && \
    mkdir ./components/tidb_query_datatype/src && echo '' > ./components/tidb_query_datatype/src/lib.rs && \
    mkdir ./components/tikv_alloc/src && echo '' > ./components/tikv_alloc/src/lib.rs && \
    mkdir ./components/tikv_util/src && echo '' > ./components/tikv_util/src/lib.rs && \
    mkdir ./components/tipb_helper/src && echo '' > ./components/tipb_helper/src/lib.rs && \
    make build_dist_release && \
    rm -rf ./target/release/.fingerprint/codec-* && \
    rm -rf ./target/release/.fingerprint/engine-* && \
    rm -rf ./target/release/.fingerprint/log_wrappers-* && \
    rm -rf ./target/release/.fingerprint/match_template-* && \
    rm -rf ./target/release/.fingerprint/panic_hook-* && \
    rm -rf ./target/release/.fingerprint/tidb_query-* && \
    rm -rf ./target/release/.fingerprint/tidb_query_codegen-* && \
    rm -rf ./target/release/.fingerprint/tidb_query_datatype-* && \
    rm -rf ./target/release/.fingerprint/tikv_alloc-* && \
    rm -rf ./target/release/.fingerprint/tikv_util-* && \
    rm -rf ./target/release/.fingerprint/tipb_helper-* && \
    rm -rf ./target/release/.fingerprint/tikv-*

# Build real binaries now
COPY ./src ./src
COPY ./components ./components

RUN make build_dist_release

# Strip debug info to reduce the docker size, may strip later?
# RUN strip --strip-debug /tikv/target/release/tikv-server && \
#     strip --strip-debug /tikv/target/release/tikv-ctl

FROM pingcap/alpine-glibc

COPY --from=toolchain /bin/tikv-ctl /tikv-ctl
COPY --from=toolchain /bin/tikv-server /tikv-server
COPY --from=toolchain /bin/tikv-importer /tikv-importer

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
