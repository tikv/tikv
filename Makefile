ENABLE_FEATURES ?= default

DEPS_PATH = $(CURDIR)/tmp
BIN_PATH = $(CURDIR)/bin

.PHONY: all

all: format build test

dev:
	@export ENABLE_FEATURES=dev && make

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

rocksdb: 
	DEPS_PATH=$(DEPS_PATH) ./scripts/build_rocksdb.sh

$(BIN_PATH)/etcd: 
	@DEPS_PATH=$(DEPS_PATH) BIN_PATH=$(BIN_PATH) ./scripts/build_etcd.sh

etcd: $(BIN_PATH)/etcd

$(BIN_PATH)/pd-server: 
	@DEPS_PATH=$(DEPS_PATH) BIN_PATH=$(BIN_PATH) ./scripts/build_pd.sh

pd: $(BIN_PATH)/pd-server

$(BIN_PATH)/tidb-server: 
	@DEPS_PATH=$(DEPS_PATH) BIN_PATH=$(BIN_PATH) ./scripts/build_tidb.sh

tidb: $(BIN_PATH)/tidb-server

deps: rocksdb etcd pd tidb

install: deps release
	@cp -f ./target/release/tikv-server $(BIN_PATH)

clean_etcd:
	@rm -f $(BIN_PATH)/etcd

clean_pd:
	@rm -f $(BIN_PATH)/pd-server

clean_tidb:
	@rm -f $(BIN_PATH)/tidb-server

clean:
	cargo clean
	@rm -rf $(DEPS_PATH) $(BIN_PATH)