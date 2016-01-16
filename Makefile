.PHONY: all

all: format build test

build:
	cargo build

run:
	cargo run

test:
	RUST_BACKTRACE=1 cargo test -- --nocapture

format:
	rustfmt --write-mode overwrite src/lib.rs src/bin/*.rs
