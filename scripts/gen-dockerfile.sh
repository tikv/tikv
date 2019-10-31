
cat <<EOT
FROM pingcap/rust as builder

WORKDIR /tikv
EOT

# Install Rust
cat <<EOT
COPY rust-toolchain ./
RUN rustup self update
RUN rustup set profile minimal
RUN rustup default $(cat "rust-toolchain")
EOT

# Use Makefile to build
cat <<EOT
COPY Makefile ./
EOT

# For cargo
cat <<EOT
COPY scripts/run-cargo.sh ./scripts/run-cargo.sh
COPY etc/cargo.config.dist ./etc/cargo.config.dist
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
