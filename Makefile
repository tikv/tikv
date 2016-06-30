ENABLE_FEATURES ?= dev

DEPS_PATH = $(CURDIR)/tmp
BIN_PATH = $(CURDIR)/bin

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

format:
	@cargo fmt -- --write-mode diff | grep "Diff at line" > /dev/null && cargo fmt -- --write-mode overwrite | grep -v "found TODO" || exit 0
	@rustfmt --write-mode diff tests/tests.rs benches/benches.rs | grep "Diff at line" > /dev/null && rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs | grep -v "found TODO" || exit 0

deps_path:
	@mkdir -p $(DEPS_PATH)
	@mkdir -p $(BIN_PATH)

rocksdb: deps_path
	DEPS_PATH=$(DEPS_PATH) ./scripts/build_rocksdb.sh

etcd: deps_path
	@DEPS_PATH=$(DEPS_PATH) BIN_PATH=$(BIN_PATH) ./scripts/build_etcd.sh

pd: deps_path
	@DEPS_PATH=$(DEPS_PATH) BIN_PATH=$(BIN_PATH) ./scripts/build_pd.sh

tidb: deps_path
	@DEPS_PATH=$(DEPS_PATH) BIN_PATH=$(BIN_PATH) ./scripts/build_tidb.sh

deps: rocksdb etcd pd tidb

install: release deps
	@cp -f ./target/release/tikv-server $(BIN_PATH)

clean:
	cargo clean
	@rm -rf $(DEPS_PATH) $(BIN_PATH)