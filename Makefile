.PHONY: all

all: format build test

build:
	cargo build

run:
	cargo run

test:
	# todo remove ulimit once issue #372 of mio is resolved.
	ulimit -n 4096 && LOG_LEVEL=DEBUG RUST_BACKTRACE=1 cargo test -- --nocapture

bench:
	# todo remove ulimit once issue #372 of mio is resolved.
	ulimit -n 10240 && LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench -- --nocapture
	ulimit -n 10240 && cargo run --release --bin bench-tikv

genprotobuf:
	cd ./src/proto && protoc --rust_out . *.proto

format:
	@cargo fmt -- --write-mode overwrite | grep -v "found TODO" || exit 0
	@rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs | grep -v "found TODO" || exit 0
