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

# start etcd
etcd &
sleep 3s

export ETCD_ENDPOINTS=127.0.0.1:2379
export ENABLE_FEATURES=default
export LOG_FILE=tests.log
make test 2>&1 | tee tests.out
status=$?
for case in `cat tests.out | python -c "import sys
handle = False
for l in sys.stdin:
    l = l.strip()
    if l == 'failures:':
        handle = True
    elif len(l) == 0:
        handle = False
    elif handle:
        print l.split(':')[-1]
"`; do
    echo find fail cases: $case
    grep $case $LOG_FILE | cut -d ' ' -f 2-
    echo
done
exit $status
