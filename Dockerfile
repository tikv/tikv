FROM pingcap/rust

MAINTAINER siddontang

ADD . /tikv

RUN cd /tikv && \
    cargo build --release && \
    cp -f target/release/tikv-server /tikv-server && \
    cargo clean

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
