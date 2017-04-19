#!/usr/bin/env bash
# Copyright 2016 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -o pipefail

panic() {
    echo -e "$@" >&1
    exit 1
}

if [[ "$SKIP_FORMAT_CHECK" != "true" ]]; then
    make format
    git diff-index --quiet HEAD -- || panic "\e[35mplease make format before creating a pr!!!\e[0m" 
fi

trap 'kill $(jobs -p) &> /dev/null || true' EXIT

if [[ "$ENABLE_FEATURES" = "" ]]; then
    export ENABLE_FEATURES=dev
fi
export LOG_FILE=tests.log
if [[ "$TRAVIS" = "true" ]]; then
    export RUST_TEST_THREADS=2
fi
export RUSTFLAGS=-Dwarnings

if [[ `uname` == "Linux" ]]; then
    export EXTRA_CARGO_ARGS="-j 2"
fi

if [[ "$SKIP_TESTS" != "true" ]]; then
    make test 2>&1 | tee tests.out
else
    export EXTRA_CARGO_ARGS="$EXTRA_CARGO_ARGS --no-run"
    make test
    exit $?
fi
status=$?
git diff-index --quiet HEAD -- || echo "\e[35mplease run tests before creating a pr!!!\e[0m" 
for case in `cat tests.out | python -c "import sys
import re
p = re.compile(\"thread '([^']+)' panicked at\")
cases = set()
for l in sys.stdin:
    l = l.strip()
    m = p.search(l)
    if m:
        cases.add(m.group(1).split(':')[-1])
print ('\n'.join(cases))
"`; do
    echo find fail cases: $case
    grep $case $LOG_FILE | cut -d ' ' -f 2-
    # there is a thread panic, which should not happen.
    status=1
    echo
done

rm $LOG_FILE || true
# don't remove the tests.out, coverage counts on it.
if [[ "$TRAVIS" != "true" ]]; then
    rm tests.out || true
fi

exit $status
