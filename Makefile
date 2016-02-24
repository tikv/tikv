.PHONY: all

all: format build test

build:
	cargo build

run:
	cargo run

test:
	RUST_LOG=tikv=DEBUG RUST_BACKTRACE=1 cargo test -- --nocapture

bench:
	RUST_LOG=tikv=ERROR RUST_BACKTRACE=1 cargo bench -- --nocapture

genprotobuf:
	cd ./src/proto && protoc --rust_out . *.proto

format:
	@cargo fmt -- --write-mode overwrite | grep -v "found TODO" || exit 0
	@rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs | grep -v "found TODO" || exit 0
