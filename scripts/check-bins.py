#!/usr/bin/env python

import sys, json, os, time, re

WHITE_LIST = {
    "configuration", "configuration_derive", "match_template", "tidb_query_codegen",
    "panic_hook", "fuzz", "fuzzer_afl", "fuzzer_honggfuzz", "fuzzer_libfuzzer",
}

JEMALLOC_SYMBOL = ["je_arena_boot", " malloc"]

def pr(s):
    if sys.stdout.isatty():
        sys.stdout.write("\x1b[2K\r" + s)
        sys.stdout.flush()
    else:
        print(s.strip())

def check_jemalloc(executable):
    p = os.popen("readelf -s " + executable)
    found = set()
    for line in p.readlines():
        line = line.strip()
        for symbol in JEMALLOC_SYMBOL:
            if line.endswith(symbol):
                found.add(symbol)
    if len(found) != len(JEMALLOC_SYMBOL):
        pr("error: %s does not contain jemalloc\n" % executable)
        sys.exit(1)

def check_sse(executable):
    p = os.popen("nm -n " + executable)
    lines = p.readlines()
    segments = [(pos, pos + 1) for (pos, l) in enumerate(lines) if "Fast_CRC32" in l]
    if len(segments) == 0:
        pr("error: %s does not contain sse4.2\n" % executable)
        sys.exit(1)
    opcode_pattern = re.compile(".*f2.*0f 38.*crc32")
    for start, end in segments:
        s_addr = lines[start].split()[0]
        e_addr = lines[end].split()[0]
        p = os.popen("objdump -d %s --start-address 0x%s --stop-address 0x%s" % (executable, s_addr, e_addr))
        for l in p.readlines():
            if opcode_pattern.search(l):
                matched = True
                break
        else:
            pr("error %s does not contain sse4.2\n" % executable)

def check_tests():
    print("Checking bins for jemalloc")
    start = time.clock()
    for line in sys.stdin.readlines():
        if not line.startswith('{'):
            continue

        data = json.loads(line)
        if not data.get("executable"):
            continue
        
        executable = data["executable"]
        name = executable.rsplit("/", 1)[1]
        if name.split('-')[0] in WHITE_LIST:
            pr("Skipping whitelisted %s" % name)
            continue

        pr("Checking binary %s" % name)
        check_jemalloc(executable)
    pr("")
    print("Done, takes %.2fs." % (time.clock() - start))

def check_release():
    print("Checking bins for jemalloc and SSE4.2")
    for arg in sys.argv[2:]:
        pr("checking binary %s" % arg)
        check_jemalloc(arg)
        check_sse(arg)
        pr("%s jemalloc sse4.2 \033[32menabled\033[0m\n" % arg)

def main():
    if sys.argv[1] == "--check-tests":
        check_tests()
    elif sys.argv[1] == "--check-release":
        check_release()

main()
