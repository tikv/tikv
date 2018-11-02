# pingcap/rust: 2017-06-12
FROM pingcap/rust as builder

MAINTAINER Liu Yin <liuy@pingcap.com>

COPY . /tikv
WORKDIR /tikv

RUN make


FROM pingcap/alpine-glibc
COPY --from=builder /tikv/bin/tikv-server /tikv-server

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
