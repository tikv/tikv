#!/bin/bash

set -e

if [[ $(uname -s) == "Darwin" ]]; then
  echo "Kernel is Darwin, change build type to debug"
  echo ""
  verified_commit="c2ca2ce22f9ee3be2cb505ea9a2faa7f892c5d45"
  echo "checkout to verified commit ${verified_commit}"
  git fetch origin ${verified_commit}
  git checkout -q FETCH_HEAD
  make release
else
  export PROXY_BUILD_TYPE=release
  export PROXY_PROFILE=release
  make build
fi
