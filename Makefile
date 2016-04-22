ENABLE_FEATURES ?= dev

.PHONY: all

all: format build test

build:
	cargo build --features ${ENABLE_FEATURES}

local-build: deps-lib build
	@echo "dependencies build done"

deps-lib: deps-dir
	export LD_LIBRARY_PATH="./deps/libs:$LD_LIBRARY_PATH"
	export LIBRARY_PATH="$LD_LIBRARY_PATH"

deps-build: deps-dir dep-gflags dep-rocksdb

deps-dir:
	mkdir -p deps/libs

dep-gflags: deps-dir
	wget https://github.com/gflags/gflags/archive/v2.1.2.tar.gz -O deps/gflags.tar.gz
	tar -zxf deps/gflags.tar.gz -C deps
	cd deps/gflags-2.1.2 && cmake . && make && cp lib/* ../libs

dep-rocksdb: deps-dir
	wget https://github.com/facebook/rocksdb/archive/rocksdb-4.3.1.tar.gz -O deps/rocksdb-4.3.1.tar.gz
	tar -xzf deps/rocksdb-4.3.1.tar.gz -C deps
	cd deps/rocksdb-rocksdb-4.3.1 && make shared_lib
	cp deps/rocksdb-rocksdb-4.3.1/librocksdb.so* deps/libs

clean-deps:
	rm -rf deps

run:
	cargo run --features ${ENABLE_FEATURES}

release:
	cargo build --release --bin tikv-server

test:
	# todo remove ulimit once issue #372 of mio is resolved.
	ulimit -n 2000 && LOG_LEVEL=DEBUG RUST_BACKTRACE=1 cargo test --features ${ENABLE_FEATURES} -- --nocapture 

bench:
	# todo remove ulimit once issue #372 of mio is resolved.
	ulimit -n 4096 && LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --features ${ENABLE_FEATURES} -- --nocapture 
	ulimit -n 4096 && RUST_BACKTRACE=1 cargo run --release --bin bench-tikv --features ${ENABLE_FEATURES}

genprotobuf:
	cd ./src/proto && protoc --rust_out . *.proto

format:
	@cargo fmt -- --write-mode overwrite | grep -v "found TODO" || exit 0
	@rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs | grep -v "found TODO" || exit 0

clean:
	cargo clean
