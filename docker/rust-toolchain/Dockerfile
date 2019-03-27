# This Docker image contains a minimal build environment for TiKV.
#
# It contains all the tools necessary to reproduce official production builds of TiKV.

FROM centos:7.6.1810

RUN yum install -y epel-release

RUN yum clean all \
	&& yum makecache \
	&& yum update -y \
	&& yum install -y tar wget git which file unzip python-pip openssl-devel \
		make cmake3 gcc gcc-c++ libstdc++-static pkg-config psmisc gdb \
		libdwarf-devel elfutils-libelf-devel elfutils-devel binutils-devel \
	&& yum clean all

RUN ln -s /usr/bin/cmake3 /usr/bin/cmake

RUN curl -L https://github.com/gflags/gflags/archive/v2.1.2.tar.gz -o gflags.tar.gz \
	&& tar xf gflags.tar.gz \
	&& cd gflags-2.1.2 \
	&& cmake . \
	&& make -j \
	&& make install \
	&& rm -rf /gflags.tar.gz /gflags-2.1.2

ENV GOLANG_VERSION 1.10.2
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 4b677d698c65370afa33757b6954ade60347aaca310ea92a63ed717d7cb0c2ff

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

RUN go get golang.org/x/tools/go/gcexportdata \
        && go get github.com/golang/lint/golint

RUN curl https://sh.rustup.rs -sSf | sh -s -- --no-modify-path --default-toolchain none -y

COPY rust-toolchain /rust-toolchain

RUN /root/.cargo/bin/rustup default $(tail -n 1 rust-toolchain) \
	&& /root/.cargo/bin/rustup component add rustfmt --toolchain $(tail -n 1 rust-toolchain) \
	&& /root/.cargo/bin/rustup component add clippy --toolchain $(tail -n 1 rust-toolchain)

ENV PROTOC_ZIP=protoc-3.3.0-linux-x86_64.zip
RUN curl -OL https://github.com/google/protobuf/releases/download/v3.3.0/$PROTOC_ZIP \
	&& unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
	&& rm -f $PROTOC_ZIP

ENV PATH /root/.cargo/bin/:$PATH
ENV LIBRARY_PATH /usr/local/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

ENTRYPOINT ["/bin/bash"]

