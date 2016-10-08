SHELL=/bin/bash -o pipefail

${LOCAL_DIR}/lib/libgflags.a:
	cd /tmp && \
	curl -L https://github.com/gflags/gflags/archive/v2.1.2.tar.gz -o gflags.tar.gz && \
	tar xf gflags.tar.gz && \
	cd gflags-2.1.2 && \
	cmake -DCMAKE_INSTALL_PREFIX=${LOCAL_DIR} . && \
	make && \
	make install

${LOCAL_DIR}/lib/librocksdb.dylib: |prepare_brew
${LOCAL_DIR}/lib/librocksdb.so: ${LOCAL_DIR}/lib/libgflags.a
${LOCAL_DIR}/lib/librocksdb.dylib ${LOCAL_DIR}/lib/librocksdb.so:
	export CPLUS_INCLUDE_PATH="${CPLUS_INCLUDE_PATH}:${LOCAL_DIR}/include" && \
	export CXX=${COMPILER} && \
	cd /tmp && \
	curl -L https://github.com/facebook/rocksdb/archive/4.12.fb.tar.gz -o rocksdb.tar.gz && \
	tar xf rocksdb.tar.gz && \
	cd rocksdb-4.12.fb && \
	INSTALL_PATH=${LOCAL_DIR} make install-shared

# TODO: use offical archive once https://github.com/SimonKagstrom/kcov/pull/146 is resolved and published.
${LOCAL_DIR}/bin/kcov:
	cd /tmp && \
	curl -L https://github.com/BusyJay/kcov/archive/busyjay/support-setting-stripped-path.tar.gz -o kcov.tar.gz && \
	tar xf kcov.tar.gz && \
	cd kcov-busyjay-support-setting-stripped-path && \
	cmake -DCMAKE_INSTALL_PREFIX=${LOCAL_DIR} . && \
	make && \
	make install

ifeq ($(TRAVIS_OS_NAME),linux)
GO_ARCH = linux
else
GO_ARCH = darwin
endif

${LOCAL_DIR}/bin/pd-server:
	mkdir -p /tmp/goroot && \
	cd /tmp/goroot && \
	curl -L https://storage.googleapis.com/golang/go1.7.${GO_ARCH}-amd64.tar.gz -o go.tar.gz && \
	tar xf go.tar.gz && \
	export GOPATH=/tmp/gopath && \
	export GOROOT=/tmp/goroot/go && \
	export PATH=/tmp/goroot/go/bin:${PATH} && \
	mkdir -p /tmp/gopath/src/github.com/pingcap && \
	cd /tmp/gopath/src/github.com/pingcap && \
	git clone --depth=1 https://github.com/pingcap/pd && \
	cd pd && \
	make build && \
	mkdir -p ${LOCAL_DIR}/bin && \
	cp bin/pd-server ${LOCAL_DIR}/bin/pd-server

prepare_rust:
	sh ~/rust/lib/rustlib/uninstall.sh && sh ~/rust-installer/rustup.sh --date=2016-10-06 --prefix=~/rust --disable-sudo --channel=nightly

prepare_linux: ${LOCAL_DIR}/lib/librocksdb.so ${LOCAL_DIR}/bin/kcov ${LOCAL_DIR}/bin/pd-server prepare_rust

prepare_brew:
	brew update && \
	brew install gflags snappy

prepare_osx: ${LOCAL_DIR}/lib/librocksdb.dylib ${LOCAL_DIR}/bin/pd-server prepare_rust | prepare_brew

test_linux test_osx:
	ulimit -n 2000 && ./travis-build/test.sh

cover_linux:
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	export RUST_TEST_THREADS=1 && \
	grep " Running " tests.out | sed -e 's/Running//g' | xargs -n 1 -i ${LOCAL_DIR}/bin/kcov --verify --coveralls-id=${TRAVIS_JOB_ID} --include-pattern tikv/src --exclude-pattern tikv/src/bin --strip-path `pwd`/ target/kcov {} --nocapture

cover_osx:
