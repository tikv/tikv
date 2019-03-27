# This Docker image contains a minimal `tikv-importer` built in **release** mode.

FROM tikv/tikv as collection

FROM pingcap/alpine-glibc

COPY --from=collection /tikv-importer /tikv-importer

EXPOSE 20160

ENTRYPOINT ["/tikv-importer"]