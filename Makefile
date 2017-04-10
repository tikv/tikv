ENABLE_FEATURES ?= default

ifeq ($(ROCKSDB_SYS_STATIC),1)
ENABLE_FEATURES += static-link
endif

ifeq ($(ROCKSDB_SYS_PORTABLE),1)
ENABLE_FEATURES += portable
endif

ifeq ($(ROCKSDB_SYS_SSE),1)
ENABLE_FEATURES += sse
endif

PROJECT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

DEPS_PATH = $(CURDIR)/tmp
BIN_PATH = $(CURDIR)/bin
GOROOT ?= $(DEPS_PATH)/go
CARGO_TARGET_DIR ?= $(CURDIR)/target

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
	@mkdir -p ${BIN_PATH}
	cp -f ${CARGO_TARGET_DIR}/release/tikv-ctl ${CARGO_TARGET_DIR}/release/tikv-server ${BIN_PATH}/

static_release:
	ROCKSDB_SYS_STATIC=1 ROCKSDB_SYS_PORTABLE=1 ROCKSDB_SYS_SSE=1  make release

# unlike test, this target will trace tests and output logs when fail test is detected.
trace_test:
	export CI=true && \
	export SKIP_FORMAT_CHECK=true && \
	${PROJECT_DIR}/travis-build/test.sh

test:
	# When SIP is enabled, DYLD_LIBRARY_PATH will not work in subshell, so we have to set it
	# again here. LOCAL_DIR is defined in .travis.yml.
	export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${LOCAL_DIR}/lib" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	cargo test --features "${ENABLE_FEATURES}" ${NO_RUN} -- --nocapture && \
	cargo test --features "${ENABLE_FEATURES}" --bench benches ${NO_RUN} -- --nocapture && \
	export MALLOC_CONF=prof:true,prof_active:false && \
	cargo test --features "${ENABLE_FEATURES}" ${NO_RUN} --bin tikv-server -- --nocapture --ignored
	# TODO: remove above target once https://github.com/rust-lang/cargo/issues/2984 is resolved.

bench:
	LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --features "${ENABLE_FEATURES}" -- --nocapture && \
	RUST_BACKTRACE=1 cargo run --release --bin bench-tikv --features "${ENABLE_FEATURES}"

format:
	@cargo fmt -- --write-mode diff | grep -E "Diff .*at line" > /dev/null && cargo fmt -- --write-mode overwrite || exit 0
	@rustfmt --write-mode diff tests/tests.rs benches/benches.rs | grep -E "Diff .*at line" > /dev/null && rustfmt --write-mode overwrite tests/tests.rs benches/benches.rs || exit 0

clean:
	cargo clean
