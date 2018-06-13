ENABLE_FEATURES ?= default

# Disable portable on MacOS to sidestep the compiler bug in clang 4.9
ifeq ($(shell uname -s),Darwin)
ROCKSDB_SYS_PORTABLE=0
endif

# Build portable binary by default unless disable explicitly
ifneq ($(ROCKSDB_SYS_PORTABLE),0)
ENABLE_FEATURES += portable
endif

# Enable sse4.2 by default unless disable explicitly
ifneq ($(ROCKSDB_SYS_SSE),0)
ENABLE_FEATURES += sse
endif

ifneq ($(FAIL_POINT),1)
ENABLE_FEATURES += no-fail
endif

PROJECT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

DEPS_PATH = $(CURDIR)/tmp
BIN_PATH = $(CURDIR)/bin
GOROOT ?= $(DEPS_PATH)/go
CARGO_TARGET_DIR ?= $(CURDIR)/target

default: release

.PHONY: all

all: format build test

pre-clippy:
	if [ "`cat clippy-version`" != "`cargo clippy --version || echo 0`" ]; then\
		cargo install clippy --version `cat clippy-version`;\
	fi


clippy: pre-clippy
	cargo clippy

dev: format clippy
	@env FAIL_POINT=1 make test

build:
	cargo build --features "${ENABLE_FEATURES}"

run:
	cargo run --features "${ENABLE_FEATURES}"

release:
	@cargo build --release --features "${ENABLE_FEATURES}"
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/tikv-ctl ${CARGO_TARGET_DIR}/release/tikv-fail ${CARGO_TARGET_DIR}/release/tikv-server ${CARGO_TARGET_DIR}/release/tikv-importer ${BIN_PATH}/

unportable_release:
	ROCKSDB_SYS_PORTABLE=0 make release

prof_release:
	ENABLE_FEATURES=mem-profiling make release

fail_release:
	FAIL_POINT=1 make release

# unlike test, this target will trace tests and output logs when fail test is detected.
trace_test:
	env CI=true SKIP_FORMAT_CHECK=true FAIL_POINT=1 ${PROJECT_DIR}/ci-build/test.sh

test: SHELL=/bin/bash
test:
	# When SIP is enabled, DYLD_LIBRARY_PATH will not work in subshell, so we have to set it
	# again here. LOCAL_DIR is defined in .travis.yml.
	export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${LOCAL_DIR}/lib" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	cargo test --features "${ENABLE_FEATURES}" ${EXTRA_CARGO_ARGS} -- --nocapture && \
	cargo test --features "${ENABLE_FEATURES}" --bench benches ${EXTRA_CARGO_ARGS} -- --nocapture  && \
	if [[ "`uname`" == "Linux" ]]; then \
		export MALLOC_CONF=prof:true,prof_active:false && \
		cargo test --features "${ENABLE_FEATURES}" ${EXTRA_CARGO_ARGS} --bin tikv-server -- --nocapture --ignored; \
	fi
	# TODO: remove above target once https://github.com/rust-lang/cargo/issues/2984 is resolved.

bench:
	LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --features "${ENABLE_FEATURES}" -- --nocapture && \
	RUST_BACKTRACE=1 cargo run --release --bin bench-tikv --features "${ENABLE_FEATURES}"

pre-format:
	rustup component add rustfmt-preview

format: pre-format
	@cargo fmt --all -- --write-mode diff >/dev/null || \
	cargo fmt --all

clean:
	@cargo clean
