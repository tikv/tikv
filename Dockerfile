# pingcap/rust: 2017-06-12
FROM pingcap/rust@sha256:781bde5503c57ca24cc828c30c161fc7b674a462b39c029b89747a90f120ee94

MAINTAINER Liu Yin <liuy@pingcap.com>

ADD . /tikv

RUN cd /tikv && \
    make && \
    cp -f bin/tikv-server /tikv-server && \
    rm -rf /tikv

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
