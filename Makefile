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

SHELL := /bin/bash
ENABLE_FEATURES ?=

# Pick an allocator
ifeq ($(TCMALLOC),1)
ENABLE_FEATURES += tcmalloc
else ifeq ($(MIMALLOC),1)
ENABLE_FEATURES += mimalloc
else ifeq ($(SYSTEM_ALLOC),1)
# no feature needed for system allocator
else
ENABLE_FEATURES += jemalloc
endif

# Disable portable on MacOS to sidestep the compiler bug in clang 4.9
ifeq ($(shell uname -s),Darwin)
ROCKSDB_SYS_PORTABLE=0
RUST_TEST_THREADS ?= 2
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

# Use Prost instead of rust-protobuf to encode and decode protocol buffers.
ifeq ($(PROST),1)
ENABLE_FEATURES += prost-codec
else
ENABLE_FEATURES += protobuf-codec
endif

PROJECT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

BIN_PATH = $(CURDIR)/bin
CARGO_TARGET_DIR ?= $(CURDIR)/target

# Build-time environment, captured for reporting by the application binary
BUILD_INFO_GIT_FALLBACK := "Unknown (no git or not git repo)"
BUILD_INFO_RUSTC_FALLBACK := "Unknown"
export TIKV_ENABLE_FEATURES := ${ENABLE_FEATURES}
export TIKV_BUILD_TIME := $(shell date -u '+%Y-%m-%d %I:%M:%S')
export TIKV_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})
export TIKV_BUILD_GIT_HASH ?= $(shell git rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_GIT_TAG ?= $(shell git describe --tag || echo ${BUILD_INFO_GIT_FALLBACK})
export TIKV_BUILD_GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})

export DOCKER_IMAGE_NAME ?= "pingcap/tikv"
export DOCKER_IMAGE_TAG ?= "latest"

# Turn on cargo pipelining to add more build parallelism. This has shown decent
# speedups in TiKV.
#
# https://internals.rust-lang.org/t/evaluating-pipelined-rustc-compilation/10199/68
export CARGO_BUILD_PIPELINING=true

default: release

.PHONY: all

clean:
	cargo clean
	rm -rf bin dist


## Development builds
## ------------------

all: format build test

dev: format clippy
	@env FAIL_POINT=1 make test

build: export TIKV_PROFILE=debug
build:
	cargo build --no-default-features --features "${ENABLE_FEATURES}"

## Release builds (optimized dev builds)
## ----------------------------

# These builds are heavily optimized, but only use thinLTO, not full
# LTO, and they don't include debuginfo by default.

# An optimized build suitable for development and benchmarking, by default built
# with RocksDB compiled with the "portable" option, for -march=x86-64 (an
# sse2-level instruction set), but with sse4.2 and the PCLMUL instruction
# enabled (the "sse" option)
release: export TIKV_PROFILE=release
release:
	cargo build --release --no-default-features --features "${ENABLE_FEATURES}"

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
.PHONY: dist_artifacts
dist_artifacts: dist_tarballs

# Build gzipped tarballs of the binaries and docker images.
# Used to build a `dist/` folder containing the release artifacts.
.PHONY: dist_tarballs
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
.PHONY: docker-tag
docker-tag: docker-tag-with-git-hash docker-tag-with-git-tag

# Tag docker images with the git hash
.PHONY: docker-tag-with-git-hash
docker-tag-with-git-hash:
	docker tag pingcap/tikv pingcap/tikv:${TIKV_BUILD_GIT_HASH}

# Tag docker images with the git tag
.PHONY: docker-tag-with-git-tag
docker-tag-with-git-tag:
	docker tag pingcap/tikv pingcap/tikv:${TIKV_BUILD_GIT_TAG}

## Testing
## -------

# Run tests under a variety of conditions. This should pass before
# submitting pull requests. Note though that the CI system tests TiKV
# through its own scripts and does not use this rule.
.PHONY: run-test
run-test:
	# When SIP is enabled, DYLD_LIBRARY_PATH will not work in subshell, so we have to set it
	# again here. LOCAL_DIR is defined in .travis.yml.
	# The special linux case below is testing the mem-profiling
	# features in tikv_alloc, which are marked #[ignore] since
	# they require special compile-time and run-time setup
	# Forturately rebuilding with the mem-profiling feature will only
	# rebuild starting at jemalloc-sys.
	# TODO: remove cd commands after https://github.com/rust-lang/cargo/issues/5364 is resolved.
	export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${LOCAL_DIR}/lib" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	cargo test --workspace --features "${ENABLE_FEATURES} mem-profiling" ${EXTRA_CARGO_ARGS} -- --nocapture && \
	if [[ "`uname`" == "Linux" ]]; then \
		export MALLOC_CONF=prof:true,prof_active:false && \
		cargo test --features "${ENABLE_FEATURES} mem-profiling" ${EXTRA_CARGO_ARGS} -p tikv_alloc -- --nocapture --ignored; \
	fi

.PHONY: test
test: run-test
	@if [[ "`uname`" = "Linux" ]]; then \
		env EXTRA_CARGO_ARGS="--message-format=json-render-diagnostics -q --no-run" make run-test |\
                python scripts/check-bins.py --features "${ENABLE_FEATURES}" --check-tests; \
	fi

## Static analysis
## ---------------

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi

pre-format: unset-override
	@rustup component add rustfmt

format: pre-format
	@cargo fmt --all -- --check >/dev/null || \
	cargo fmt --all

pre-clippy: unset-override
	@rustup component add clippy

ALLOWED_CLIPPY_LINTS=-A clippy::module_inception -A clippy::needless_pass_by_value -A clippy::cognitive_complexity \
	-A clippy::unreadable_literal -A clippy::should_implement_trait -A clippy::verbose_bit_mask \
	-A clippy::implicit_hasher -A clippy::large_enum_variant -A clippy::new_without_default \
	-A clippy::neg_cmp_op_on_partial_ord -A clippy::too_many_arguments \
	-A clippy::excessive_precision -A clippy::collapsible_if -A clippy::blacklisted_name \
	-A clippy::needless_range_loop -A clippy::redundant_closure \
	-A clippy::match_wild_err_arm -A clippy::blacklisted_name -A clippy::redundant_closure_call \
	-A clippy::identity_conversion -A clippy::new_ret_no_self

# PROST feature works differently in test cdc and backup package, they need to be checked under their folders.
ifneq (,$(findstring prost-codec,"$(ENABLE_FEATURES)"))
clippy: pre-clippy
	@cargo clippy --all --exclude cdc --exclude backup --exclude tests --exclude cmd \
		--exclude fuzz-targets --exclude fuzzer-honggfuzz --exclude fuzzer-afl --exclude fuzzer-libfuzzer \
		--all-targets --no-default-features \
		--features "${ENABLE_FEATURES}" -- $(ALLOWED_CLIPPY_LINTS)
	@for pkg in "components/cdc" "components/backup" "cmd" "tests"; do \
		cd $$pkg && \
		cargo clippy --all-targets --no-default-features \
			--features "${ENABLE_FEATURES}" -- $(ALLOWED_CLIPPY_LINTS) && \
		cd - >/dev/null;\
	done
	@for pkg in "fuzz" "fuzz/fuzzer-afl" "fuzz/fuzzer-honggfuzz" "fuzz/fuzzer-libfuzzer"; do \
		cd $$pkg && \
		cargo clippy --all-targets -- $(ALLOWED_CLIPPY_LINTS) && \
		cd - >/dev/null; \
	done
else
clippy: pre-clippy
	@cargo clippy --workspace --features "${ENABLE_FEATURES}" -- $(ALLOWED_CLIPPY_LINTS)
endif

pre-audit:
	$(eval LATEST_AUDIT_VERSION := $(strip $(shell cargo search cargo-audit | head -n 1 | awk '{ gsub(/"/, "", $$3); print $$3 }')))
	$(eval CURRENT_AUDIT_VERSION = $(strip $(shell (cargo audit --version 2> /dev/null || echo "noop 0") | awk '{ print $$2 }')))
	@if [ "$(LATEST_AUDIT_VERSION)" != "$(CURRENT_AUDIT_VERSION)" ]; then \
		cargo install cargo-audit --force; \
	fi

# Check for security vulnerabilities
audit: pre-audit
	cargo audit

.PHONY: check-udeps
check-udeps:
	which cargo-udeps &>/dev/null || cargo install cargo-udeps && cargo udeps

FUZZER ?= Honggfuzz

.PHONY: fuzz
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

# A special target for building TiKV docker image.
.PHONY: docker
docker:
	bash ./scripts/gen-dockerfile.sh | docker build \
		-t ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} \
		-f - . \
		--build-arg GIT_HASH=${TIKV_BUILD_GIT_HASH} \
		--build-arg GIT_TAG=${TIKV_BUILD_GIT_TAG} \
		--build-arg GIT_BRANCH=${TIKV_BUILD_GIT_BRANCH}

## The driver for script/run-cargo.sh
## ----------------------------------

# Cargo only has two non-test profiles, dev and release, and we have
# more than two use cases for which a cargo profile is required. This
# is a hack to manage more cargo profiles, written in
# `etc/cargo.config.*`. These make use of the unstable
# `-Zconfig-profile` cargo option to specify profiles in
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
x-build-dist: export X_PACKAGE=cmd
x-build-dist:
	bash scripts/run-cargo.sh
