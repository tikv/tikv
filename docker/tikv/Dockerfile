# This Docker image contains minimal binaries for:
#
# * `tikv-server`
# * `tikv-ctl`
# * `tikv-importer`
#
# All TiKV tools are built in **release** mode.
FROM tikv/rust-toolchain as toolchain

ENV ROCKSDB_SYS_SSE 1

COPY . /tikv
WORKDIR ./tikv

RUN make build_dist_release

# Strip debug info to reduce the docker size, may strip later?
# RUN strip --strip-debug /tikv/target/release/tikv-server && \
#     strip --strip-debug /tikv/target/release/tikv-ctl

FROM pingcap/alpine-glibc

COPY --from=toolchain /tikv/target/release/tikv-ctl /tikv-ctl
COPY --from=toolchain /tikv/target/release/tikv-server /tikv-server

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
