# The TiKV Makefile
#
# These are mostly light rules around cargo and in general developers
# can do daily development using cargo alone. It does do a few key
# things that developers need to understand:
#
# It turns on features that are not on by default in the cargo
# manifest but that are part of the release build (see the
# ENABLE_FEATURES variable).
#
# The `clippy` rule runs clippy with custom configuration, so
# is the correct way to run clippy against TiKV.
#
# The `test` rule runs tests in configurations that are not covered by `cargo
# test` by default.
#
# Important make rules:
#
# - `build` - create a development profile, unoptimized build
#
# - `run` - run a development build
#
# - `test` - run the test suite in a variety of configurations
#
# - `format` - reformat the code with cargo format
#
# - `clippy` - run clippy with tikv-specific configuration
#
# - `dev` - the rule that needs to pass before submitting a PR. It runs
#   tests and static analysis including clippy and rustfmt
#
# - `release` - create a release profile, optimized build

SHELL := bash
ENABLE_FEATURES ?=

# Frame pointer is enabled by default. The purpose is to provide stable and
# reliable stack backtraces (for CPU Profiling).
#
# If you want to disable frame-pointer, please manually set the environment
# variable `PROXY_FRAME_POINTER=0 make` (This will fallback to `libunwind`
# based stack backtrace.).
#
# Note that enabling frame-pointer means that the Rust standard library will
# be recompiled.
ifndef PROXY_FRAME_POINTER
ARCH=$(shell uname -m)
export PROXY_FRAME_POINTER=1
# Disable when x86, See https://github.com/pingcap/tiflash/issues/6091
ifeq ($(ARCH),i386)
export PROXY_FRAME_POINTER=0
endif
ifeq ($(ARCH),i686)
export PROXY_FRAME_POINTER=0
endif
ifeq ($(ARCH),x86_64)
export PROXY_FRAME_POINTER=0
endif
endif

ifeq ($(PROXY_FRAME_POINTER),1)
export RUSTFLAGS := $(RUSTFLAGS) -Cforce-frame-pointers=yes
export CFLAGS := $(CFLAGS) -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer
export CXXFLAGS := $(CXXFLAGS) -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer
ENABLE_FEATURES += pprof-fp
endif

# Pick an allocator
ifeq ($(TCMALLOC),1)
ENABLE_FEATURES += tcmalloc
else ifeq ($(MIMALLOC),1)
ENABLE_FEATURES += mimalloc
else ifeq ($(SNMALLOC),1)
ENABLE_FEATURES += snmalloc
else ifeq ($(SYSTEM_ALLOC),1)
# no feature needed for system allocator
else
endif

# Disable portable on macOS to sidestep the compiler bug in clang 4.9
ifeq ($(shell uname -s),Darwin)
ROCKSDB_SYS_PORTABLE=0
RUST_TEST_THREADS ?= 2
endif

# Disable SSE on ARM
ifeq ($(shell uname -p),aarch64)
ROCKSDB_SYS_SSE=0
endif
ifeq ($(shell uname -p),arm)
ROCKSDB_SYS_SSE=0
endif
ifeq ($(shell uname -p),arm64)
ROCKSDB_SYS_SSE=0
endif

# Build portable binary by default unless disable explicitly
ifneq ($(ROCKSDB_SYS_PORTABLE),0)
ENABLE_FEATURES += portable
endif

# Enable sse4.2 by default unless disable explicitly
# Note this env var is also tested by scripts/check-sse4_2.sh
ifneq ($(ROCKSDB_SYS_SSE),0)
ENABLE_FEATURES += sse
endif

ifeq ($(FAIL_POINT),1)
ENABLE_FEATURES += failpoints
endif

# Set the storage engines used for testing
ifneq ($(NO_DEFAULT_TEST_ENGINES),1)
ENABLE_FEATURES += test-engine-kv-rocksdb test-engine-raft-raft-engine
else
# Caller is responsible for setting up test engine features
endif

ifneq ($(NO_CLOUD),1)
ENABLE_FEATURES += cloud-aws
ENABLE_FEATURES += cloud-gcp
ENABLE_FEATURES += cloud-azure
endif

PROJECT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

BIN_PATH = $(CURDIR)/bin
CARGO_TARGET_DIR ?= $(CURDIR)/target

# Build-time environment, captured for reporting by the application binary
BUILD_INFO_GIT_FALLBACK := "Unknown (no git or not git repo)"
BUILD_INFO_RUSTC_FALLBACK := "Unknown"
export PROXY_BUILD_TIME := $(shell date -u '+%Y-%m-%d %H:%M:%S')
export PROXY_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})
export PROXY_BUILD_GIT_HASH ?= $(shell git rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export PROXY_BUILD_GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})

export DOCKER_IMAGE_NAME ?= "pingcap/tikv"
export DOCKER_IMAGE_TAG ?= "latest"

# Turn on cargo pipelining to add more build parallelism. This has shown decent
# speedups in TiKV.
#
# https://internals.rust-lang.org/t/evaluating-pipelined-rustc-compilation/10199/68
export CARGO_BUILD_PIPELINING=true

# Compiler gave us the following error message when using a specific version of gcc on
# aarch64 architecture and PROXY_FRAME_POINTER=1:
#     .../atomic.rs: undefined reference to __aarch64_xxx
# This is a temporary workaround.
# See: https://github.com/rust-lang/rust/issues/93166
#      https://bugzilla.redhat.com/show_bug.cgi?id=1830472
ifeq ($(TIKV_BUILD_RUSTC_TARGET),aarch64-unknown-linux-gnu)
export RUSTFLAGS := $(RUSTFLAGS) -Ctarget-feature=-outline-atomics
endif

# Almost all the rules in this Makefile are PHONY
# Declaring a rule as PHONY could improve correctness
# But probably instead just improves performance by a little bit
.PHONY: audit clippy format pre-format pre-clippy pre-audit unset-override
.PHONY: all build clean dev check-udeps doc error-code fuzz run test
.PHONY: docker docker-tag docker-tag-with-git-hash docker-tag-with-git-tag
.PHONY: ctl dist_artifacts dist_tarballs x-build-dist
.PHONY: build_dist_release dist_release dist_unportable_release
.PHONY: fail_release prof_release release unportable_release


default: release

clean:
	cargo clean
	rm -rf bin dist


## Development builds
## ------------------

all: format build test

dev: format clippy
	@env FAIL_POINT=1 make test

ifeq ($(PROXY_FRAME_POINTER),1)
build: ENABLE_FEATURES += pprof-fp
endif
build:
	PROXY_ENABLE_FEATURES="${ENABLE_FEATURES}" ./build.sh

debug: export PROXY_PROFILE=debug
debug:
	make build

release: export PROXY_PROFILE=release
release:
	make build

## Testing
## -------

# Run tests under a variety of conditions. This should pass before
# submitting pull requests.
test:
	./scripts/test-all

## Static analysis
## ---------------

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi

pre-format: unset-override
	@rustup component add rustfmt
	@cargo install --force -q cargo-sort

ci_fmt_check:
	M="fmt" ./ci_check.sh

ci_test:
	M="testold" ./ci_check.sh
	M="testnew" ./ci_check.sh
	make debug

gen_proxy_ffi: pre-format
	./gen-proxy-ffi.sh

format: pre-format
	@cargo fmt
	@cargo sort -w ./Cargo.toml ./*/Cargo.toml components/*/Cargo.toml cmd/*/Cargo.toml >/dev/null

fmt:
	@cargo fmt
	@cargo sort -w ./Cargo.toml ./*/Cargo.toml components/*/Cargo.toml cmd/*/Cargo.toml >/dev/null

pre-clippy: unset-override
	@rustup component add clippy

clippy: pre-clippy
	@./scripts/check-redact-log
	@./scripts/check-docker-build
	@./scripts/clippy-all
