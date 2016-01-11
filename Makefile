.PHONY: all

all: format build test

build:
	cargo build

run:
	cargo run

test:
	cargo test -- --nocapture

format:
	rustfmt --write-mode overwrite src/lib.rs src/bin/*.rs
