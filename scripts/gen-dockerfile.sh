# TiKV root 
dir="."
output="./Dockerfile"

if [ "$#" -eq 1 ]; then
    output=$1
fi

cat <<EOT > ${output}
FROM pingcap/rust as builder

RUN mkdir -p /tikv 
WORKDIR /tikv

# Install Rust
COPY rust-toolchain ./
RUN rustup default $(cat "rust-toolchain")

# Install dependencies at first
COPY Cargo.toml Cargo.lock ./

# Remove fuzz and test workspace
RUN sed -i '/fuzz/d' Cargo.toml && \\
    sed -i '/test\_/d' Cargo.toml 

EOT

# Get components
components=$(ls -d ${dir}/components/*  | xargs -n 1 basename | grep -v "test")

# List components and add their Cargo files
echo "# Add components Cargo files
# Notice: every time we add a new component, we must regenerate the dockerfile" >> ${output}

for i in ${components}; do 
    echo "COPY ${dir}/components/${i}/Cargo.toml ./components/${i}/Cargo.toml" >> ${output}
done


cat <<EOT >> ${output}

# Create dummy files, build the dependencies
# then remove TiKV fingerprint for following rebuild
RUN mkdir -p ./src/bin && \\
    echo 'fn main() {}' > ./src/bin/tikv-ctl.rs && \\
    echo 'fn main() {}' > ./src/bin/tikv-server.rs && \\
    echo 'fn main() {}' > ./src/bin/tikv-importer.rs && \\
    echo '' > ./src/lib.rs && \\
EOT

for i in ${components}; do 
    echo "    mkdir ./components/${i}/src && echo '' > ./components/${i}/src/lib.rs && \\" >> ${output}
done

echo '    cargo build --no-default-features --release --features "jemalloc portable sse no-fail" && \' >> ${output}

for i in ${components}; do 
    echo "    rm -rf ./target/release/.fingerprint/${i}-* && \\" >> ${output}
done

echo "    rm -rf ./target/release/.fingerprint/tikv-*" >> ${output}

cat <<EOT >> ${output}

# Build real binaries now
COPY ${dir}/src ./src
COPY ${dir}/components ./components

RUN cargo build --no-default-features --release --features "jemalloc portable sse no-fail" 

# Strip debug info to reduce the docker size, may strip later?
# RUN strip --strip-debug /tikv/target/release/tikv-server && \\
#     strip --strip-debug /tikv/target/release/tikv-ctl

FROM pingcap/alpine-glibc
COPY --from=builder /tikv/target/release/tikv-server /tikv-server
COPY --from=builder /tikv/target/release/tikv-ctl /tikv-ctl

EXPOSE 20160 20180

ENTRYPOINT ["/tikv-server"]
EOT