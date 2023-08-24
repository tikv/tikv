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
# variable `TIKV_FRAME_POINTER=0 make` (This will fallback to `libunwind`
# based stack backtrace.).
#
# Note that enabling frame-pointer means that the Rust standard library will
# be recompiled.
ifndef TIKV_FRAME_POINTER
export TIKV_FRAME_POINTER=1
endif

ifeq ($(TIKV_FRAME_POINTER),1)
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
ENABLE_FEATURES += jemalloc

# Only tested on Linux
ifeq ($(shell uname -s),Linux)
ENABLE_FEATURES += mem-profiling
# According to jemalloc/jemalloc#585, enabling it on some platform or some
# versions of glibc can cause deadlock.
# export JEMALLOC_SYS_WITH_MALLOC_CONF = prof:true,prof_active:false
endif
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
export TIKV_ENABLE_FEATURES := ${ENABLE_FEATURES}
export TIKV_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})
export TIKV_BUILD_RUSTC_TARGET := $(shell rustc -vV | awk '/host/ { print $$2 }')
export TIKV_BUILD_GIT_HASH ?= $(shell git rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_GIT_TAG ?= $(shell git describe --tag || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})

export DOCKER_IMAGE_NAME ?= "pingcap/tikv"
export DOCKER_IMAGE_TAG ?= "latest"
export DEV_DOCKER_IMAGE_NAME ?= "pingcap/tikv_dev"

# Turn on cargo pipelining to add more build parallelism. This has shown decent
# speedups in TiKV.
#
# https://internals.rust-lang.org/t/evaluating-pipelined-rustc-compilation/10199/68
export CARGO_BUILD_PIPELINING=true

# Compiler gave us the following error message when using a specific version of gcc on
# aarch64 architecture and TIKV_FRAME_POINTER=1:
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

all: format build test error-code

dev: format clippy
	@env FAIL_POINT=1 make test

build: export TIKV_PROFILE=debug
ifeq ($(TIKV_FRAME_POINTER),1)
build:
	rustup component add rust-src
	cargo build --no-default-features --features "${ENABLE_FEATURES}" \
		-Z build-std=core,std,alloc,proc_macro,test \
		-Z unstable-options \
		--target "${TIKV_BUILD_RUSTC_TARGET}" \
		--out-dir "${CARGO_TARGET_DIR}/debug"
else
build:
	cargo build --no-default-features --features "${ENABLE_FEATURES}"
endif

## Release builds (optimized dev builds)
## ----------------------------

# These builds are heavily optimized, but only use thinLTO, not full
# LTO, and they don't include debuginfo by default.

# An optimized build suitable for development and benchmarking, by default built
# with RocksDB compiled with the "portable" option, for -march=x86-64 (an
# sse2-level instruction set), but with sse4.2 and the PCLMUL instruction
# enabled (the "sse" option)
release: export TIKV_PROFILE=release
ifeq ($(TIKV_FRAME_POINTER),1)
release:
	rustup component add rust-src
	cargo build --release --no-default-features --features "${ENABLE_FEATURES}" \
		-Z build-std=core,std,alloc,proc_macro,test \
		-Z unstable-options \
		--target "${TIKV_BUILD_RUSTC_TARGET}" \
		--out-dir "${CARGO_TARGET_DIR}/release"
else
release:
	cargo build --release --no-default-features --features "${ENABLE_FEATURES}"
endif

# An optimized build that builds an "unportable" RocksDB, which means it is
# built with -march native. It again includes the "sse" option by default.
unportable_release:
	ROCKSDB_SYS_PORTABLE=0 make release

# An optimized build with jemalloc memory profiling enabled.
prof_release:
	ENABLE_FEATURES=mem-profiling make release

# An optimized build instrumented with failpoints.
# This is used for schrodinger chaos testing.
fail_release:
	FAIL_POINT=1 make dist_release

## Distribution builds (true release builds)
## -------------------

# These builds are fully optimized, with LTO, and they contain
# debuginfo. They take a very long time to build, so it is recommended
# not to use them.

# The target used by CI/CD to build the distributable release artifacts.
# Individual developers should only need to use the `dist_` rules when working
# on the CI/CD system.
dist_release:
	make build_dist_release
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/tikv-ctl ${CARGO_TARGET_DIR}/release/tikv-server ${BIN_PATH}/
ifeq ($(shell uname),Linux) # Macs binary isn't elf format
	@python scripts/check-bins.py --features "${ENABLE_FEATURES}" --check-release ${BIN_PATH}/tikv-ctl ${BIN_PATH}/tikv-server
endif

# Build with release flag as if it were for distribution, but without
# additional sanity checks and file movement.
build_dist_release: export TIKV_PROFILE=dist_release
build_dist_release:
	make x-build-dist
ifeq ($(shell uname),Linux) # Macs don't have objcopy
	# Reduce binary size by compressing binaries.
	# FIXME: Currently errors with `Couldn't find DIE referenced by DW_AT_abstract_origin`
	# dwz ${CARGO_TARGET_DIR}/release/tikv-server
	# FIXME: https://sourceware.org/bugzilla/show_bug.cgi?id=24764
	# dwz ${CARGO_TARGET_DIR}/release/tikv-ctl
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/tikv-server
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/tikv-ctl
endif

# Distributable bins with SSE4.2 optimizations
dist_unportable_release:
	ROCKSDB_SYS_PORTABLE=0 make dist_release

# Create distributable artifacts. Binaries and Docker image tarballs.
dist_artifacts: dist_tarballs

# Build gzipped tarballs of the binaries and docker images.
# Used to build a `dist/` folder containing the release artifacts.
dist_tarballs: docker
	docker rm -f tikv-binary-extraction-dummy || true
	docker create --name tikv-binary-extraction-dummy pingcap/tikv
	mkdir -p dist bin
	docker cp tikv-binary-extraction-dummy:/tikv-server bin/tikv-server
	docker cp tikv-binary-extraction-dummy:/tikv-ctl bin/tikv-ctl
	tar -czf dist/tikv.tar.gz bin/*
	docker save pingcap/tikv | gzip > dist/tikv-docker.tar.gz
	docker rm tikv-binary-extraction-dummy

# Create tags of the docker images
docker-tag: docker-tag-with-git-hash docker-tag-with-git-tag

# Tag docker images with the git hash
docker-tag-with-git-hash:
	docker tag pingcap/tikv pingcap/tikv:${TIKV_BUILD_GIT_HASH}

# Tag docker images with the git tag
docker-tag-with-git-tag:
	docker tag pingcap/tikv pingcap/tikv:${TIKV_BUILD_GIT_TAG}


## Execution environment
## -------
## Run a command in the environment setup by the Makefile
##
##     COMMAND="echo" make run
##
run:
	@env MAKEFILE_RUN=1 $(COMMAND)

## Testing
## -------

# Run tests under a variety of conditions. This should pass before
# submitting pull requests.
test:
	./scripts/test-all -- --nocapture

# Run tests with nextest.
ifndef CUSTOM_TEST_COMMAND
test_with_nextest: export CUSTOM_TEST_COMMAND=nextest run
endif
test_with_nextest: export RUSTDOCFLAGS="-Z unstable-options --persist-doctests"
test_with_nextest:
	./scripts/test-all

## Static analysis
## ---------------

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi

pre-format: unset-override
	@rustup component add rustfmt
	@which cargo-sort &> /dev/null || cargo install -q cargo-sort

format: pre-format
	@cargo fmt
	@cargo sort -w -c &>/dev/null || cargo sort -w >/dev/null

doc:
	@cargo doc --workspace --document-private-items \
		--exclude fuzz-targets --exclude fuzzer-honggfuzz --exclude fuzzer-afl --exclude fuzzer-libfuzzer \
		--no-default-features --features "${ENABLE_FEATURES}"

pre-clippy: unset-override
	@rustup component add clippy

clippy: pre-clippy
	@./scripts/check-redact-log
	@./scripts/check-log-style
	@./scripts/check-docker-build
	@./scripts/check-license
	@./scripts/clippy-all

pre-audit:
	$(eval LATEST_AUDIT_VERSION := $(strip $(shell cargo search cargo-audit | head -n 1 | awk '{ gsub(/"/, "", $$3); print $$3 }')))
	$(eval CURRENT_AUDIT_VERSION = $(strip $(shell (cargo audit --version 2> /dev/null || echo "noop 0") | awk '{ print $$2 }')))
	@if [ "$(LATEST_AUDIT_VERSION)" != "$(CURRENT_AUDIT_VERSION)" ]; then \
		cargo install cargo-audit --force; \
	fi

# Check for security vulnerabilities
audit: pre-audit
	cargo audit

check-udeps:
	which cargo-udeps &>/dev/null || cargo install cargo-udeps && cargo udeps

FUZZER ?= Honggfuzz

fuzz:
	@cargo run --package fuzz --no-default-features --features "${ENABLE_FEATURES}" -- run ${FUZZER} ${FUZZ_TARGET} \
	|| echo "" && echo "Set the target for fuzzing using FUZZ_TARGET and the fuzzer using FUZZER (default is Honggfuzz)"

## Special targets
## ---------------

# A special target for building just the tikv-ctl binary and release mode and copying it
# into BIN_PATH. It's not clear who uses this for what. If you know please document it.
ctl:
	cargo build --release --no-default-features --features "${ENABLE_FEATURES}" --bin tikv-ctl
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/tikv-ctl ${BIN_PATH}/

# Actually use make to track dependencies! This saves half a second.
error_code_files := $(shell find $(PROJECT_DIR)/components/error_code/ -type f )
etc/error_code.toml: $(error_code_files)
	cargo run --manifest-path components/error_code/Cargo.toml

error-code: etc/error_code.toml

# A special target for building TiKV docker image.
docker:
	docker build \
		-t ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} \
		--build-arg GIT_HASH=${TIKV_BUILD_GIT_HASH} \
		--build-arg GIT_TAG=${TIKV_BUILD_GIT_TAG} \
		--build-arg GIT_BRANCH=${TIKV_BUILD_GIT_BRANCH} \
		.

docker_test:
	docker build -f Dockerfile.test \
		-t ${DEV_DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} \
		.
	docker run -i -v $(shell pwd):/tikv \
		${DEV_DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} \
		make test

## The driver for script/run-cargo.sh
## ----------------------------------

# Cargo only has two non-test profiles, dev and release, and we have
# more than two use cases for which a cargo profile is required. This
# is a hack to manage more cargo profiles, written in `etc/cargo.config.*`.
# So we use cargo `config-profile` feature to specify profiles in
# `.cargo/config`, which `scripts/run-cargo.sh copies into place.
#
# Presently the only thing this is used for is the `dist_release`
# rules, which are used for producing release builds.

DIST_CONFIG=etc/cargo.config.dist

ifneq ($(DEBUG),)
export X_DEBUG=${DEBUG}
endif

export X_CARGO_ARGS:=${CARGO_ARGS}

x-build-dist: export X_CARGO_CMD=build
x-build-dist: export X_CARGO_FEATURES=${ENABLE_FEATURES}
x-build-dist: export X_CARGO_RELEASE=1
x-build-dist: export X_CARGO_CONFIG_FILE=${DIST_CONFIG}
x-build-dist: export X_CARGO_TARGET_DIR=${CARGO_TARGET_DIR}
x-build-dist: export X_PACKAGE=tikv-server tikv-ctl
x-build-dist:
	bash scripts/run-cargo.sh
