FROM pingcap/rust

MAINTAINER Liu Yin <liuy@pingcap.com>

ADD . /tikv

RUN cd /tikv && \
    make && \
    cp -f bin/tikv-server /tikv-server && \
    rm -rf /tikv

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
