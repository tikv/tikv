#!/bin/bash

echo "building tidb..."
rm -rf ${DEPS_PATH}/src/github.com/pingcap/tidb
git clone --depth=1 https://github.com/pingcap/tidb.git ${DEPS_PATH}/src/github.com/pingcap/tidb

cd ${DEPS_PATH}/src/github.com/pingcap/tidb
export GOPATH=$DEPS_PATH
make server
cp -f ./tidb-server/tidb-server $BIN_PATH