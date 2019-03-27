# This Docker image contains a minimal `tikv-ctl` built in **release** mode.

FROM tikv/tikv as collection

FROM pingcap/alpine-glibc

COPY --from=collection /tikv-ctl /tikv-ctl

ENTRYPOINT ["/tikv-ctl"]