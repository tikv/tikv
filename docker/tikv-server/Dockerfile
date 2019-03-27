# This Docker image contains a minimal `tikv-server` built in **release** mode.

FROM tikv/tikv as collections

FROM pingcap/alpine-glibc

COPY --from=collections /tikv-server /tikv-server

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
