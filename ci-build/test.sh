#!/usr/bin/env bash
# Copyright 2016 TiKV Project Authors

set -eo pipefail

panic() {
    echo -e "$@" >&2
    exit 1
}

# Move to project root
cd "$(dirname "$0")/.."

if [[ -z "$SKIP_CHECK_DIRTY_TESTS" ]]; then
    make format
    git --no-pager diff --exit-code HEAD || panic "\e[35mplease 'make format' before creating a pr!!!\e[0m"
fi

if [[ -z "$SKIP_CHECK_DIRTY_TESTS" ]]; then
    make error-code
    git --no-pager diff --exit-code HEAD || panic "\e[35mplease 'make format' before creating a pr!!!\e[0m"
fi

trap 'kill $(jobs -p) &> /dev/null || true' EXIT

if [[ "$TRAVIS" = "true" ]]; then
    export RUST_TEST_THREADS=2
fi
export RUSTFLAGS=-Dwarnings

make clippy || panic "\e[35mplease fix the 'make clippy' errors!!!\e[0m"

set +e
export LOG_FILE=tests.log
if [[ -z "$SKIP_TESTS" ]]; then
    make test 2>&1 | tee tests.out
else
    EXTRA_CARGO_ARGS="--no-run" make test
    exit $?
fi
status=$?
if [[ -z "$SKIP_CHECK_DIRTY_TESTS" ]]; then
    git --no-pager diff --exit-code HEAD || panic "\e[35mplease run 'make test' before creating a pr!!!\e[0m"
fi
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
