SHELL := /bin/bash
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

BUILD_INFO_GIT_FALLBACK := "Unknown (no git or not git repo)"
BUILD_INFO_RUSTC_FALLBACK := "Unknown"
export TIKV_BUILD_TIME := $(shell date -u '+%Y-%m-%d %I:%M:%S')
export TIKV_BUILD_GIT_HASH := $(shell git rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})

default: release

.PHONY: all

all: format build test

pre-clippy: unset-override
	@rustup component add clippy-preview

clippy: pre-clippy
	@cargo clippy --all --all-targets -- \
		-A module_inception -A needless_pass_by_value -A cyclomatic_complexity \
		-A unreadable_literal -A should_implement_trait -A verbose_bit_mask \
		-A implicit_hasher -A large_enum_variant -A new_without_default \
		-A new_without_default_derive -A neg_cmp_op_on_partial_ord \
		-A too_many_arguments -A excessive_precision -A collapsible_if \
		-A blacklisted_name

dev: format clippy
	@env FAIL_POINT=1 make test

build:
	cargo build --features "${ENABLE_FEATURES}"

ctl:
	@cargo build --release --features "${ENABLE_FEATURES}" --bin tikv-ctl
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/tikv-ctl ${BIN_PATH}/

run:
	cargo run --features "${ENABLE_FEATURES}" --bin tikv-server

release:
	@cargo build --release --features "${ENABLE_FEATURES}"
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/tikv-ctl ${CARGO_TARGET_DIR}/release/tikv-server ${CARGO_TARGET_DIR}/release/tikv-importer ${BIN_PATH}/

unportable_release:
	ROCKSDB_SYS_PORTABLE=0 make release

prof_release:
	ENABLE_FEATURES=mem-profiling make release

fail_release:
	FAIL_POINT=1 make release

# unlike test, this target will trace tests and output logs when fail test is detected.
trace_test:
	env CI=true SKIP_FORMAT_CHECK=true FAIL_POINT=1 ${PROJECT_DIR}/ci-build/test.sh

test:
	# When SIP is enabled, DYLD_LIBRARY_PATH will not work in subshell, so we have to set it
	# again here. LOCAL_DIR is defined in .travis.yml.
	export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${LOCAL_DIR}/lib" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	cargo test --features "${ENABLE_FEATURES}" --all ${EXTRA_CARGO_ARGS} -- --nocapture && \
	cargo test --features "${ENABLE_FEATURES}" --bench misc ${EXTRA_CARGO_ARGS} -- --nocapture  && \
	if [[ "`uname`" == "Linux" ]]; then \
		export MALLOC_CONF=prof:true,prof_active:false && \
		cargo test --features "${ENABLE_FEATURES}" ${EXTRA_CARGO_ARGS} --bin tikv-server -- --nocapture --ignored; \
	fi
	# TODO: remove above target once https://github.com/rust-lang/cargo/issues/2984 is resolved.

bench:
	LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo bench --all --features "${ENABLE_FEATURES}" -- --nocapture

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi

pre-format: unset-override
	@rustup component add rustfmt-preview

format: pre-format
	@cargo fmt --all -- --check >/dev/null || \
	cargo fmt --all

clean:
	@cargo clean

expression: format clippy
	LOG_LEVEL=ERROR RUST_BACKTRACE=1 cargo test --features "${ENABLE_FEATURES}" "coprocessor::dag::expr" -- --nocapture
