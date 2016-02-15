.PHONY: all

all: format build test

build:
	cargo build

run:
	cargo run

test:
	RUST_LOG=tikv=DEBUG RUST_BACKTRACE=1 cargo test -- --nocapture

bench:
	RUST_BACKTRACE=1 cargo bench -- --nocapture

genprotobuf:
	cd ./src/proto && protoc --rust_out . *.proto

format:
	rustfmt --write-mode overwrite src/lib.rs src/bin/*.rs tests/tests.rs
