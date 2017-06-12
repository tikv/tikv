# pingcap/rust: 2017-06-12
FROM pingcap/rust@sha256:90b973eb3461667762cbe7c0f989ccfee760c6a954b85e668d8a25a23938f287

MAINTAINER Liu Yin <liuy@pingcap.com>

ADD . /tikv

RUN cd /tikv && \
    make && \
    cp -f bin/tikv-server /tikv-server && \
    rm -rf /tikv

EXPOSE 20160

ENTRYPOINT ["/tikv-server"]
