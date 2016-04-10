.PHONY: all

all: format build test

build:
	cargo build --features "dev"

run:
	cargo run --features "dev"

release:
	cargo build --release --bin tikv-server

test:
	# todo remove ulimit once issue #372 of mio is resolved.
	ulimit -n 2000 && LOG_LEVEL=DEBUG RUST_BACKTRACE=1 cargo test --features "dev" -- --nocapture 

bench:
	# todo remove ulimit once issue #372 of mio is resolved.
	ulimit -n 4096 && LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --features "dev" -- --nocapture 
	ulimit -n 4096 && RUST_BACKTRACE=1 cargo run --release --bin bench-tikv --features "dev"

genprotobuf:
	cd ./src/proto && protoc --rust_out . *.proto

format:
	@cargo fmt -- --write-mode overwrite | grep -v "found TODO" || exit 0
	@rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs | grep -v "found TODO" || exit 0

clean:
	cargo clean
