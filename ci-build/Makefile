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

# TODO: use official archive once https://github.com/SimonKagstrom/kcov/pull/146 is resolved and published.
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

prepare_linux: ${LOCAL_DIR}/bin/kcov

prepare_brew:
	brew update && \
	brew install gflags snappy gperftools

prepare_osx: prepare_brew ${LOCAL_DIR}/bin/kcov

test_linux test_osx:
	export CI=true && ${CI_BUILD_DIR}/test.sh

cover_linux cover_osx:
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	export RUST_TEST_THREADS=2 && \
	grep " Running " tests.out | sed -e 's/Running//g' | xargs -n 1 -I {} ${LOCAL_DIR}/bin/kcov --verify --coveralls-id=${TRAVIS_JOB_ID} --include-pattern tikv/src --exclude-pattern tikv/src/bin --strip-path `pwd`/ target/kcov {} --nocapture
