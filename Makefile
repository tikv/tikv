ENABLE_FEATURES ?= default

DEPS_PATH = $(CURDIR)/tmp
BIN_PATH = $(CURDIR)/bin
GOROOT ?= $(DEPS_PATH)/go

default: release

.PHONY: all

all: format build test

dev:
	@export ENABLE_FEATURES=dev && make all

build:
	cargo build --features ${ENABLE_FEATURES}

run:
	cargo run --features ${ENABLE_FEATURES}

release:
	cargo build --release --bin tikv-server
	@mkdir -p bin 
	cp -f ./target/release/tikv-server ./bin

test:
	# Default Mac OSX `ulimit -n` is 256, too small. 
	ulimit -n 2000 && LOG_LEVEL=DEBUG RUST_BACKTRACE=1 cargo test --features ${ENABLE_FEATURES} -- --nocapture
	# TODO: remove following target once https://github.com/rust-lang/cargo/issues/2984 is resolved.
	ulimit -n 2000 && LOG_LEVEL=DEBUG RUST_BACKTRACE=1 cargo test --features ${ENABLE_FEATURES} --bench benches -- --nocapture 

bench:
	# Default Mac OSX `ulimit -n` is 256, too small. 
	ulimit -n 4096 && LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --features ${ENABLE_FEATURES} -- --nocapture 
	ulimit -n 4096 && RUST_BACKTRACE=1 cargo run --release --bin bench-tikv --features ${ENABLE_FEATURES}

format:
	@cargo fmt -- --write-mode diff | grep "Diff at line" > /dev/null && cargo fmt -- --write-mode overwrite || exit 0
	@rustfmt --write-mode diff tests/tests.rs benches/benches.rs | grep "Diff at line" > /dev/null && rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs || exit 0

clean:
	cargo clean
