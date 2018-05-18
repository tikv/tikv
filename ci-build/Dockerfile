FROM ubuntu:latest
ENV PATH $PATH:/root/.cargo/bin

# Install all dependencies
RUN apt-get -qq update -y
RUN apt-get install -y -qq curl clang git make cmake golang python

# Cache source, we limit this heavily to avoid loading a large context.
RUN mkdir -p build
COPY RUST_VERSION /build/RUST_VERSION
COPY Cargo.lock /build/Cargo.lock
COPY Cargo.toml /build/Cargo.toml
COPY src /build/src
COPY fuzz/Cargo.toml /build/fuzz/Cargo.toml


WORKDIR /build

# Pull in Rust toolchain
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain `tail -n 1 RUST_VERSION` -y

# Ensure we have rustfmt
RUN rustup component add rustfmt-preview

# Prefetch packages
RUN cargo fetch

# Clean up
RUN rm -rf /build

# Mark a volume
VOLUME /build
