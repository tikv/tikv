# This Docker image contains a minimal `tikv-server` built in **release** mode.
FROM tikv/tikv as collection

FROM pingcap/alpine-glibc

COPY --from=collection /tikv-server /tikv-server

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
