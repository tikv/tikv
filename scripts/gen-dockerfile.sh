cat <<EOT
FROM pingcap/rust as builder

WORKDIR /tikv

# Install Rust
COPY rust-toolchain ./
RUN rustup self update
RUN rustup set profile minimal
RUN rustup default $(cat "rust-toolchain")

# Use Makefile to build
COPY Makefile ./

# For cargo
COPY scripts/run-cargo.sh ./scripts/run-cargo.sh
COPY etc/cargo.config.dist ./etc/cargo.config.dist

# Install dependencies at first
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p ./cmd/
COPY cmd/Cargo.toml ./cmd/
EOT

# Get components, remove test and profiler components
components=$(ls -d ./components/*  | xargs -n 1 basename | grep -v "test" | grep -v "profiler")

# List components and add their Cargo files
echo "# Add components Cargo files
# Notice: every time we add a new component, we must regenerate the dockerfile"

for i in ${components}; do
    echo "COPY ./components/${i}/Cargo.toml ./components/${i}/Cargo.toml"
done


cat <<EOT

# Create dummy files, build the dependencies
# then remove TiKV fingerprint for following rebuild
RUN mkdir -p ./cmd/src/bin && \\
    echo 'fn main() {}' > ./cmd/src/bin/tikv-ctl.rs && \\
    echo 'fn main() {}' > ./cmd/src/bin/tikv-server.rs && \\
    echo '' > ./cmd/src/lib.rs && \\
    mkdir -p ./src/ && \\
    echo '' > ./src/lib.rs && \\
EOT

for i in ${components}; do
    echo "    mkdir ./components/${i}/src && echo '' > ./components/${i}/src/lib.rs && \\"
done

cat <<EOT
    # Remove test dependencies and profile features.
    for cargotoml in \$(find . -name "Cargo.toml"); do \\
        sed -i '/fuzz/d' \${cargotoml} && \\
        sed -i '/test\_/d' \${cargotoml} && \\
        sed -i '/profiling/d' \${cargotoml} && \\
        sed -i '/profiler/d' \${cargotoml} ; \\
    done

EOT

echo 'RUN make build_dist_release && \'

for i in ${components}; do
    echo "    rm -rf ./target/release/.fingerprint/${i}-* && \\"
done

echo "    rm -rf ./target/release/.fingerprint/tikv-*"

cat <<EOT

# Build real binaries now
COPY ./src ./src
COPY ./cmd/src ./cmd/src
COPY ./components ./components

RUN make build_dist_release

# Strip debug info to reduce the docker size, may strip later?
# RUN strip --strip-debug /tikv/target/release/tikv-server && \\
#     strip --strip-debug /tikv/target/release/tikv-ctl

FROM pingcap/alpine-glibc
COPY --from=builder /tikv/target/release/tikv-server /tikv-server
COPY --from=builder /tikv/target/release/tikv-ctl /tikv-ctl

EXPOSE 20160 20180

ENTRYPOINT ["/tikv-server"]
EOT
