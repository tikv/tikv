FROM pingcap/alpine-glibc
COPY ./bin/tikv-server /tikv-server
COPY ./bin/tikv-ctl /tikv-ctl

EXPOSE 20160 20180

ENTRYPOINT ["/tikv-server"]
