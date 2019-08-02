# This script makes sure that the executables in the target directory
# are linked to jemalloc. It is suitable to run as part of CI.

#!/bin/bash

dirs="./target/debug ./target/release"

errors=0

# These don't need to link to jemalloc
# NB: The fuzzer bins here are just placeholders due to the workspace
# structure; they are not actual fuzzers.
whitelist="match_template tidb_query_codegen panic_hook fuzz fuzzer_afl fuzzer_honggfuzz fuzzer_libfuzzer"

if [[ "`uname`" != "Linux" ]]; then
    echo "skipping jemalloc check - not on Linux"
    exit 0
fi

echo "checking bins for jemalloc"

for dir in $dirs; do
    for file in `ls $dir 2> /dev/null`; do

	dirfile="$dir/$file"
	if [[ -x "$dirfile" && ! -d "$dirfile" ]]; then

	    # Trim off trailing -hexstuff to check whitelist
	    prefix=`echo "$file" | sed s/-.*$//`

	    skip=0
	    for white_prefix in $whitelist; do
		if [[ "$prefix" == "$white_prefix" ]]; then
		    skip=1
		    continue
		fi
	    done

	    if [[ "$skip" -eq 1 ]]; then
		echo "skipping whitelisted $dirfile"
		continue
	    fi
	    
	    echo "checking binary $dirfile for jemalloc"

	    # Make sure there's a jemalloc function inside the bin
	    # This ".*" pattern is to accommodate differences in
	    # symbol naming between old-Rust's built-in jemalloc and a
	    # manually-activated jemalloc.
	    if [[ ! `nm "$dirfile" | grep "t .*je_arena_boot$"` ]]; then
		echo "error: $dirfile does not contain jemalloc"
		errors=1
		continue
	    fi

	    # Make sure there's a malloc symbol inside the bin
	    if [[ ! `nm "$dirfile" | grep "T malloc$"` ]]; then
		echo "error: $dirfile does not contain jemalloc"
		errors=1
	    fi
	fi
    done
done

if [[ "$errors" -ne 0 ]]; then
    echo "some binaries do not link to jemalloc"
    echo "fix this by adding tikv_alloc as a dependency and importing it with 'extern crate'"
    exit 1
fi
