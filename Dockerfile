# This Docker image contains a minimal build environment for TiKV
#
# It contains all the tools necessary to reproduce develop builds of TiKV
# This file may be outdated, you can check the latest version at:
#   https://github.com/PingCAP-QE/artifacts/blob/main/dockerfiles/cd/builders/tikv/Dockerfile

# build requires:
#   - docker >= v20.10
#
# build steps:
#   - git clone --recurse-submodules https://github.com/tikv/tikv.git tikv
#   - cd tikv
#   - docker build -t tikv -f Dockerfile .

########### stage: builder
FROM quay.io/rockylinux/rockylinux:8.10.20240528-ubi as builder

# install packages.
RUN --mount=type=cache,target=/var/cache/dnf \
    dnf upgrade-minimal -y && \
    dnf --enablerepo=powertools install -y \
    dwz make git findutils gcc gcc-c++ cmake curl openssl-devel perl python3 \
    libstdc++-static

# install protoc.
# renovate: datasource=github-release depName=protocolbuffers/protobuf
ARG PROTOBUF_VER=v3.15.8
RUN FILE=$([ "$(arch)" = "aarch64" ] && echo "protoc-${PROTOBUF_VER#?}-linux-aarch_64.zip" || echo "protoc-${PROTOBUF_VER#?}-linux-$(arch).zip"); \
    curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/${PROTOBUF_VER}/${FILE}" && unzip "$FILE" -d /usr/local/ && rm -f "$FILE"

# install rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s - -y --default-toolchain none
ENV PATH /root/.cargo/bin/:$PATH

########### stage: building
FROM builder as building
COPY . /tikv
RUN --mount=type=cache,target=/tikv/target \
    ROCKSDB_SYS_STATIC=1 make dist_release -C /tikv
RUN /tikv/bin/tikv-server --version

########### stage: Final image
FROM ghcr.io/pingcap-qe/bases/tikv-base:v1.9.2

ENV MALLOC_CONF="prof:true,prof_active:false"
COPY --from=building /tikv/bin/tikv-server  /tikv-server
COPY --from=building /tikv/bin/tikv-ctl     /tikv-ctl

EXPOSE 20160 20180
ENTRYPOINT ["/tikv-server"]
