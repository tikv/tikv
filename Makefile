ENABLE_FEATURES ?= default

ifeq ($(ROCKSDB_SYS_STATIC),1)
ENABLE_FEATURES += static-link
endif

ifeq ($(ROCKSDB_SYS_PORTABLE),1)
ENABLE_FEATURES += portable
endif

DEPS_PATH = $(CURDIR)/tmp
BIN_PATH = $(CURDIR)/bin
GOROOT ?= $(DEPS_PATH)/go

default: release

.PHONY: all

all: format build test

dev:
	@export ENABLE_FEATURES=dev && make all

build:
	cargo build --features "${ENABLE_FEATURES}"

run:
	cargo run --features "${ENABLE_FEATURES}"

release:
	cargo build --release --features "${ENABLE_FEATURES}"
	@mkdir -p bin 
	cp -f ./target/release/tikv-ctl ./target/release/tikv-server ./bin

static_release:
	ROCKSDB_SYS_STATIC=1 ROCKSDB_SYS_PORTABLE=1 make release

test:
	# When SIP is enabled, DYLD_LIBRARY_PATH will not work in subshell, so we have to set it
	# again here. LOCAL_DIR is defined in .travis.yml.
	export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${LOCAL_DIR}/lib" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	cargo test --features "${ENABLE_FEATURES}" -- --nocapture && \
	cargo test --features "${ENABLE_FEATURES}" --bench benches -- --nocapture 
	# TODO: remove above target once https://github.com/rust-lang/cargo/issues/2984 is resolved.

bench:
	LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --features "${ENABLE_FEATURES}" -- --nocapture && \
	RUST_BACKTRACE=1 cargo run --release --bin bench-tikv --features "${ENABLE_FEATURES}"

format:
	@cargo fmt -- --write-mode diff | grep -E "Diff .*at line" > /dev/null && cargo fmt -- --write-mode overwrite || exit 0
	@rustfmt --write-mode diff tests/tests.rs benches/benches.rs | grep -E "Diff .*at line" > /dev/null && rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs || exit 0

clean:
	cargo clean
