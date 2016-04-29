ENABLE_FEATURES ?= dev

.PHONY: all

all: format build test

build:
	cargo build --features ${ENABLE_FEATURES}

run:
	cargo run --features ${ENABLE_FEATURES}

release:
	cargo build --release --bin tikv-server

test:
	# Default Mac OSX `ulimit -n` is 256, too small. 
	ulimit -n 2000 && LOG_LEVEL=DEBUG RUST_BACKTRACE=1 cargo test --features ${ENABLE_FEATURES} -- --nocapture 

bench:
	# Default Mac OSX `ulimit -n` is 256, too small. 
	ulimit -n 4096 && LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --features ${ENABLE_FEATURES} -- --nocapture 
	ulimit -n 4096 && RUST_BACKTRACE=1 cargo run --release --bin bench-tikv --features ${ENABLE_FEATURES}

genprotobuf:
	cd ./src/proto && protoc --rust_out . *.proto

format:
	@cargo fmt -- --write-mode overwrite | grep -v "found TODO" || exit 0
	@rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs | grep -v "found TODO" || exit 0

clean:
	cargo clean
