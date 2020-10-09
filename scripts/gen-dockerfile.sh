#! /bin/bash
# This Docker image contains a minimal build environment for TiKV
#
# It contains all the tools necessary to reproduce official production builds of TiKV

# We need to use CentOS 7 because many of our users choose this as their deploy machine.
# Since the glibc it uses (2.17) is from 2012 (https://sourceware.org/glibc/wiki/Glibc%20Timeline)
# it is our lowest common denominator in terms of distro support.

# Some commands in this script are structured in order to reduce the number of layers Docker
# generates. Unfortunately Docker is limited to only 125 layers:
# https://github.com/moby/moby/blob/a9507c6f76627fdc092edc542d5a7ef4a6df5eec/layer/layer.go#L50-L53

# We require epel packages, so enable the fedora EPEL repo then install dependencies.
# Install the system dependencies
# Attempt to clean and rebuild the cache to avoid 404s
cat <<EOT
FROM centos:7.6.1810 as builder
RUN yum install -y epel-release && \
    yum clean all && \
    yum makecache

RUN yum install -y \
        perl \
        make cmake3 dwz \
        gcc gcc-c++ libstdc++-static && \
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

# For cargo
cat <<EOT
COPY scripts ./scripts
COPY etc ./etc
COPY Cargo.lock ./Cargo.lock
EOT

# Get components, remove test and profiler components
components=($(find . -type f -name 'Cargo.toml' | sed -r 's|/[^/]+$||' | sort -u))
src_dirs=$(for i in ${components[@]}; do echo ${i}/src; done | xargs)
lib_files=$(for i in ${components[@]}; do echo ${i}/src/lib.rs; done | xargs)
# List components and add their Cargo files
echo "RUN mkdir -p ${src_dirs} && touch ${lib_files}"
for i in ${components[@]}; do
    echo "COPY ${i}/Cargo.toml ${i}/Cargo.toml"
done

# Create dummy files, build the dependencies
# then remove TiKV fingerprint for following rebuild.
# Finally, remove fuzz and profile features.
cat <<EOT
RUN mkdir -p ./cmd/src/bin && \\
    echo 'fn main() {}' > ./cmd/src/bin/tikv-ctl.rs && \\
    echo 'fn main() {}' > ./cmd/src/bin/tikv-server.rs && \\
    for cargotoml in \$(find . -name "Cargo.toml"); do \\
        sed -i '/fuzz/d' \${cargotoml} && \\
        sed -i '/profiler/d' \${cargotoml} ; \\
    done

COPY Makefile ./
RUN make build_dist_release
EOT

# Remove fingerprints for when we build the real binaries.
fingerprint_dirs=$(for i in ${components[@]}; do echo ./target/release/.fingerprint/$(basename ${i})-*; done | xargs)
echo "RUN rm -rf ${fingerprint_dirs} ./target/release/.fingerprint/tikv-*"
for i in "${components[@]:1}"; do
    echo "COPY ${i} ${i}"
done
echo "COPY src src"

# Build real binaries now
cat <<EOT
ARG GIT_FULLBACK="Unknown (no git or not git repo)"
ARG GIT_HASH=\${GIT_FULLBACK}
ARG GIT_TAG=\${GIT_FULLBACK}
ARG GIT_BRANCH=\${GIT_FULLBACK}
ENV TIKV_BUILD_GIT_HASH=\${GIT_HASH}
ENV TIKV_BUILD_GIT_TAG=\${GIT_TAG}
ENV TIKV_BUILD_GIT_BRANCH=\${GIT_BRANCH}
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
