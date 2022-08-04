#!/usr/bin/env bash

for i in {1..1000}
do
    cargo test --release -p tests --test random > random_$i.log 2>&1
    if grep -q 'test result: ok' random_$i.log; then
        grep 'total_write_count' random_$i.log;
        rm random_$i.log
    fi
done
