SHELL=/bin/bash -o pipefail

${ROCKSDB_DIR}/libgflags.a:
	cd /tmp && \
	curl -L https://github.com/gflags/gflags/archive/v2.1.2.tar.gz -o gflags.tar.gz && \
	tar xf gflags.tar.gz && \
	cd gflags-2.1.2 && \
	cmake . && \
	make && \
	mkdir -p ${ROCKSDB_DIR} && \
	cp lib/* ${ROCKSDB_DIR} && \
	cp -r include ${ROCKSDB_DIR}

${ROCKSDB_DIR}/librocksdb.dylib: prepare_brew
${ROCKSDB_DIR}/librocksdb.so: ${ROCKSDB_DIR}/libgflags.a
${ROCKSDB_DIR}/librocksdb.dylib ${ROCKSDB_DIR}/librocksdb.so:
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export CPLUS_INCLUDE_PATH="${CPLUS_INCLUDE_PATH}:${ROCKSDB_DIR}/include" && \
	export CXX=${COMPILER} && \
	cd /tmp && \
	curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.9.tar.gz -o rocksdb.tar.gz && \
	tar xf rocksdb.tar.gz && \
	cd rocksdb-rocksdb-4.9 && \
	make shared_lib && \
	cp -R librocksdb.* ${ROCKSDB_DIR}

# TODO: use offical archive once https://github.com/SimonKagstrom/kcov/pull/146 is resolved and published.
${KCOV_DIR}/kcov:
	cd /tmp && \
	curl -L https://github.com/BusyJay/kcov/archive/busyjay/support-setting-stripped-path.tar.gz -o kcov.tar.gz && \
	tar xf kcov.tar.gz && \
	cd kcov-busyjay-support-setting-stripped-path && \
	cmake . && \
	make && \
	mkdir -p ${KCOV_DIR} && \
	cp src/kcov ${KCOV_DIR}/kcov

${PD_DIR}/pd-server: prepare_go
	export GOROOT=/tmp/goroot/go && \
	export GOPATH=/tmp/gopath && \
	export PATH=$GOROOT/bin:$PATH && \
	mkdir -p /tmp/gopath/src/github.com/pingcap && \
	cd /tmp/gopath/src/github.com/pingcap && \
	git clone --depth=1 https://github.com/pingcap/pd && \
	cd pd && \
	make build && \
	mkdir -p ${PD_DIR} && \
	cp bin/pd-server ${PD_DIR}/

prepare_go:
ifeq ($(TRAVIS_OS_NAME),linux)
	mkdir -p /tmp/goroot && \
	cd /tmp/goroot && \
	curl -L https://storage.googleapis.com/golang/go1.7.linux-amd64.tar.gz -o go.tar.gz && \
	tar xf go.tar.gz
else
	mkdir -p /tmp/goroot && \
	cd /tmp/goroot && \
	curl -L https://storage.googleapis.com/golang/go1.7.darwin-amd64.tar.gz -o go.tar.gz && \
	tar xf go.tar.gz
endif

prepare_rust:
	sh ~/rust/lib/rustlib/uninstall.sh && sh ~/rust-installer/rustup.sh --date=2016-07-31 --prefix=~/rust --disable-sudo --channel=nightly

prepare_linux: ${ROCKSDB_DIR}/librocksdb.so ${KCOV_DIR}/kcov ${PD_DIR}/pd-server prepare_rust

prepare_brew:
	brew update && \
	brew install gflags snappy

prepare_osx: ${ROCKSDB_DIR}/librocksdb.dylib ${PD_DIR}/pd-server prepare_rust

test_linux test_osx:
	export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	ulimit -n 2000 && \
	./travis-build/test.sh

cover_linux:
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	export RUST_TEST_THREADS=1 && \
	grep " Running " tests.out | sed -e 's/Running//g' | xargs -n 1 -i ${KCOV_DIR}/kcov --verify --coveralls-id=${TRAVIS_JOB_ID} --include-pattern tikv/src --exclude-pattern tikv/src/bin --strip-path `pwd`/ target/kcov {} --nocapture

cover_osx:
