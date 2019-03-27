# This Docker image contains minimal binaries for:
#
# * `tikv-server`
# * `tikv-ctl`
# * `tikv-importer`
#
# All TiKV tools are built in **release** mode.
FROM tikv/rust-toolchain as toolchain

# Only copy the necessary build folders to avoid false positive rebuilds.
RUN mkdir /tikv
COPY src /tikv/src
COPY etc /tikv/etc
COPY scripts /tikv/scripts
COPY rust-toolchain /tikv/rust-toolchain
# TODO: This shouldn't be necessary
COPY fuzz /tikv/fuzz
COPY components /tikv/components
COPY Makefile /tikv/Makefile
COPY Cargo.toml /tikv/Cargo.toml
COPY Cargo.lock /tikv/Cargo.lock
WORKDIR /tikv

ENV ROCKSDB_SYS_SSE 1
RUN make release

FROM pingcap/alpine-glibc

COPY --from=toolchain /tikv/bin/tikv-ctl /tikv-ctl
COPY --from=toolchain /tikv/bin/tikv-server /tikv-server
COPY --from=toolchain /tikv/bin/tikv-importer /tikv-importer

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
