SHELL=/bin/bash -o pipefail

CI_BUILD_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

${LOCAL_DIR}/lib/libgflags.a:
	cd /tmp && \
	curl -L https://github.com/gflags/gflags/archive/v2.1.2.tar.gz -o gflags.tar.gz && \
	tar xf gflags.tar.gz && \
	cd gflags-2.1.2 && \
	cmake -DCMAKE_INSTALL_PREFIX=${LOCAL_DIR} . && \
	make && \
	make install

${LOCAL_DIR}/lib/librocksdb.dylib: |prepare_brew
${LOCAL_DIR}/lib/librocksdb.so.5.5.1: ${LOCAL_DIR}/lib/libgflags.a
${LOCAL_DIR}/lib/librocksdb.dylib ${LOCAL_DIR}/lib/librocksdb.so.5.5.1:
	export CPLUS_INCLUDE_PATH="${CPLUS_INCLUDE_PATH}:${LOCAL_DIR}/include" && \
	export CXX=${COMPILER} && \
	cd /tmp && \
	curl -L https://github.com/facebook/rocksdb/archive/v5.5.1.tar.gz -o rocksdb.tar.gz && \
	tar xf rocksdb.tar.gz && \
	cd rocksdb-5.5.1 && \
	INSTALL_PATH=${LOCAL_DIR} make install-shared

# TODO: use offical archive once https://github.com/SimonKagstrom/kcov/pull/146 is resolved and published.
${LOCAL_DIR}/bin/kcov:
	cd /tmp && \
	curl -L https://github.com/SimonKagstrom/kcov/archive/v33.tar.gz -o kcov.tar.gz && \
	tar xf kcov.tar.gz && \
	cd kcov-33 && \
	if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then \
	    cmake -G Xcode . && \
		xcodebuild -configuration Release && \
		cp src/Release/kcov ${LOCAL_DIR}/bin/kcov; \
	else \
	    cmake -DCMAKE_INSTALL_PREFIX=${LOCAL_DIR} . && \
		make && \
		make install; \
	fi

prepare-rustfmt:
	curl -L https://github.com/tennix/rustfmt/releases/download/v0.6/rustfmt-${RUSTFMT_VERSION}-linux-amd64.tar.gz -o rustfmt-${RUSTFMT_VERSION}-linux-amd64.tar.gz && \
	mkdir -p ${HOME}/.cargo/bin && tar xzf rustfmt-${RUSTFMT_VERSION}-linux-amd64.tar.gz -C ${HOME}/.cargo/bin --strip-components=1
	# cargo install --vers =0.6.0 rustfmt || exit 0

prepare_linux: ${LOCAL_DIR}/lib/librocksdb.so.5.5.1 ${LOCAL_DIR}/bin/kcov  prepare-rustfmt

prepare_brew:
	brew update && \
	brew install gflags snappy gperftools rocksdb

prepare_osx: prepare_brew ${LOCAL_DIR}/bin/kcov

test_linux test_osx:
	export CI=true && ${CI_BUILD_DIR}/test.sh

cover_linux cover_osx:
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	export RUST_TEST_THREADS=2 && \
	grep " Running " tests.out | sed -e 's/Running//g' | xargs -n 1 -I {} ${LOCAL_DIR}/bin/kcov --verify --coveralls-id=${TRAVIS_JOB_ID} --include-pattern tikv/src --exclude-pattern tikv/src/bin --strip-path `pwd`/ target/kcov {} --nocapture
