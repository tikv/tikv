#! /bin/bash
# This Docker image contains a minimal build environment for TiKV
#
# It contains all the tools necessary to reproduce official production builds of TiKV

# We need to use CentOS 7 because many of our users choose this as their deploy machine.
# Since the glibc it uses (2.17) is from 2012 (https://sourceware.org/glibc/wiki/Glibc%20Timeline)
# it is our lowest common denominator in terms of distro support.

# We require epel packages, so enable the fedora EPEL repo then install dependencies.
cat <<EOT
FROM centos:7.6.1810 as builder
RUN yum clean all && \
    yum makecache && \
    yum update -y && \
    yum install -y epel-release
EOT

# Install the system dependencies
# Attempt to clean and rebuild the cache to avoid 404s
cat <<EOT
RUN yum clean all && \
    yum makecache && \
	yum update -y && \
	yum install -y tar wget git which file unzip python-pip openssl-devel \
		make cmake3 gcc gcc-c++ libstdc++-static pkg-config psmisc gdb \
		libdwarf-devel elfutils-libelf-devel elfutils-devel binutils-devel \
        dwz && \
	yum clean all
EOT


# CentOS gives cmake 3 a weird binary name, so we link it to something more normal
# This is required by many build scripts, including ours.
cat <<EOT
RUN ln -s /usr/bin/cmake3 /usr/bin/cmake
ENV LIBRARY_PATH /usr/local/lib:\$LIBRARY_PATH
ENV LD_LIBRARY_PATH /usr/local/lib:\$LD_LIBRARY_PATH
EOT

# Install Rustup
cat <<EOT
RUN curl https://sh.rustup.rs -sSf | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:\$PATH
EOT

# Install the Rust toolchain
cat <<EOT
WORKDIR /tikv
COPY rust-toolchain ./
RUN rustup self update
RUN rustup set profile minimal
RUN rustup default \$(cat "rust-toolchain")
EOT

# Use Makefile to build
cat <<EOT
COPY Makefile ./
EOT

# For cargo
cat <<EOT
COPY scripts ./scripts
COPY etc ./etc
EOT

# Install dependencies at first
cat <<EOT
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p ./cmd/
COPY cmd/Cargo.toml ./cmd/
EOT

# Get components, remove test and profiler components
components=$(ls -d ./components/*  | xargs -n 1 basename | grep -v "test" | grep -v "profiler")
# List components and add their Cargo files
for i in ${components}; do
    echo "COPY ./components/${i}/Cargo.toml ./components/${i}/Cargo.toml"
done


# Create dummy files, build the dependencies
# then remove TiKV fingerprint for following rebuild
cat <<EOT
RUN mkdir -p ./cmd/src/bin && \\
    echo 'fn main() {}' > ./cmd/src/bin/tikv-ctl.rs && \\
    echo 'fn main() {}' > ./cmd/src/bin/tikv-server.rs && \\
    echo '' > ./cmd/src/lib.rs && \\
    mkdir -p ./src/ && \\
    echo '' > ./src/lib.rs
EOT

for i in ${components}; do
    echo "RUN mkdir ./components/${i}/src && echo '' > ./components/${i}/src/lib.rs"
done

# Remove test dependencies and profile features.
cat <<EOT
RUN for cargotoml in \$(find . -name "Cargo.toml"); do \\
        sed -i '/fuzz/d' \${cargotoml} && \\
        sed -i '/test\_/d' \${cargotoml} && \\
        sed -i '/profiling/d' \${cargotoml} && \\
        sed -i '/profiler/d' \${cargotoml} ; \\
    done
EOT

# 
echo "RUN make build_dist_release"
# Remove fingerprints for when we build the real binaries.
for i in ${components}; do
    echo "RUN rm -rf ./target/release/.fingerprint/${i}-*"
done
echo "RUN rm -rf ./target/release/.fingerprint/tikv-*"

# Build real binaries now
cat <<EOT
COPY ./src ./src
COPY ./cmd/src ./cmd/src
COPY ./components ./components
COPY ./.git ./.git
RUN make build_dist_release
EOT

# Export to a clean image
cat <<EOT
FROM pingcap/alpine-glibc
COPY --from=builder /tikv/target/release/tikv-server /tikv-server
COPY --from=builder /tikv/target/release/tikv-ctl /tikv-ctl

EXPOSE 20160 20180

ENTRYPOINT ["/tikv-server"]
EOT
