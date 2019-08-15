#!/bin/bash
#
# This script makes sure that the tikv-server executables in the target
# directory enables intel® sse4.2 extensions. It is suitable to run as part of
# CI.

# Check release only.
dirs="./target/release"

errors=0

# These need to check sse4.2.
targets="tikv-server"

if [[ "`uname`" != "Linux" ]]; then
    echo "skipping sse4.2 check - not on Linux"
    exit 0
fi

if [[ "$ROCKSDB_SYS_SSE" == "0" ]]; then
	echo "skipping sse4.2 check - sse4.2 disabled"
	exit 0
fi

echo "checking bins for sse4.2"

for dir in $dirs; do
    for file in $targets; do

        dirfile="$dir/$file"
        if [[ -x "$dirfile" && ! -d "$dirfile" ]]; then

            echo "checking binary $dirfile for sse4.2"

            # RocksDB uses sse4.2 in the `Fast_CRC32` function
            fast_crc32=$(nm "$dirfile" | grep -o " _.*Fast_CRC32.*")
            if [[ ! $fast_crc32 ]]; then
                echo "error: $dirfile does not contain rocksdb::crc32c::Fast_CRC32 function"
                errors=1
                continue
            fi

            # Make sure the `Fast_CRC32` uses the sse4.2 instruction `crc32`
            # f2.*0f 38 is the opcode of `crc32`, see Intel® SSE4 Programming Reference
            found=0
            for sym in $fast_crc32; do
		read -r start stop <<<$(nm -n "$dirfile" | grep -A1 "$sym" | awk '{printf("0x"$1" ")}')
                if [[ `objdump -d $dirfile --start-address $start --stop-address $stop 2> /dev/null | grep ".*f2.*0f 38.*crc32"` ]]; then
                    found=1
                    break
                fi
            done
            if [[ "$found" -ne 1 ]]; then
                echo "error: $dirfile does not enable sse4.2"
                errors=1
	    else
                echo -e "$dirfile sse4.2 \e[32menabled\e[0m"
            fi
        fi
    done
done

if [[ "$errors" -ne 0 ]]; then
    echo "some binaries do not enable sse4.2"
    echo "fix this by building tikv with ROCKSDB_SYS_SSE=1"
    exit 1
fi
