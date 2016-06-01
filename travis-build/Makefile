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

${ROCKSDB_DIR}/librocksdb.so: ${ROCKSDB_DIR}/libgflags.a
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export CPLUS_INCLUDE_PATH="${CPLUS_INCLUDE_PATH}:${ROCKSDB_DIR}/include" && \
	export CXX=${COMPILER} && \
	cd /tmp && \
	curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.3.1.tar.gz -o rocksdb.tar.gz && \
	tar xf rocksdb.tar.gz && \
	cd rocksdb-rocksdb-4.3.1 && \
	make shared_lib && \
	cp librocksdb.so* ${ROCKSDB_DIR}

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

prepare_linux: ${ROCKSDB_DIR}/librocksdb.so ${KCOV_DIR}/kcov

prepare_osx:
	brew update && \
	brew install gflags snappy rocksdb

test_linux:
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	ulimit -n 2000 && \
	ENABLE_FEATURES=default make test 2>&1 | tee tests.log

test_osx:
	ENABLE_FEATURES=default make test

cover_linux:
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	grep " Running " tests.log | sed -e 's/Running//g' | xargs -n 1 -i ${KCOV_DIR}/kcov --verify --coveralls-id=${TRAVIS_JOB_ID} --include-pattern tikv/src --exclude-pattern tikv/src/bin --strip-path `pwd`/ target/kcov {} --nocapture

cover_osx:
	
