#!/usr/bin/env python
#
# This script makes sure tests binary are linked to jemalloc and release binaries
# utilize sse4.2 extensions.
#
# It is suitable to run as part of CI.

import sys, json, os, time, re

# These don't need to link to jemalloc
# NB: The fuzzer bins here are just placeholders due to the workspace
# structure; they are not actual fuzzers.
WHITE_LIST = {
    "online_config", "online_config_derive", "match_template", "tidb_query_codegen",
    "panic_hook", "fuzz", "fuzzer_afl", "fuzzer_honggfuzz", "fuzzer_libfuzzer",
    "coprocessor_plugin_api", "example_plugin", "memory_trace_macros", "case_macros",
    "tracker"
}

JEMALLOC_SYMBOL = ["je_arena_boot", " malloc"]

SYS_LIB = ["libstdc++"]

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

def is_sse_enabled(features):
    return "sse" in features

# jemalloc is enabled by default
def is_jemalloc_enabled(features):
    return not features or "jemalloc" in features

def check_sse(executable):
    p = os.popen("nm -n " + executable)
    lines = p.readlines()
    segments = [(pos, pos + 1) for (pos, l) in enumerate(lines) if "Fast_CRC32" in l]
    if len(segments) == 0:
        pr("error: %s does not contain sse4.2\n" % executable)
        print("fix this by building tikv with ROCKSDB_SYS_SSE=1")
        sys.exit(1)

    # Make sure the `Fast_CRC32` uses the sse4.2 instruction `crc32`
    # f2.*0f 38 is the opcode of `crc32`, see SSE4 Programming Reference.
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
            print("fix this by building tikv with ROCKSDB_SYS_SSE=1")
            sys.exit(1)

def check_tests(features):
    if not is_jemalloc_enabled(features):
        print("jemalloc not enabled, skip check!")
        return
    else:
        print("Checking bins for jemalloc")
    start = time.time()
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
    print("Done, takes %.2fs." % (time.time() - start))

def ensure_link(args):
    p = os.popen("uname")
    if "Linux" not in p.readline():
        return
    for bin in args:
        p = os.popen("ldd " + bin)
        requires = set(l.split()[0] for l in p.readlines())
        for lib in SYS_LIB:
            if any(lib in r for r in requires):
                pr("error: %s should not requires dynamic library %s\n" % (bin, lib))
                sys.exit(1)

def check_release(enabled_features, args):
    ensure_link(args)
    checked_features = []
    if is_jemalloc_enabled(enabled_features):
        checked_features.append("jemalloc")
    if is_sse_enabled(enabled_features):
        checked_features.append("SSE4.2")
    if not checked_features:
        print("Both jemalloc and SSE4.2 are disabled, skip check")
        return
    print("Enabled features: %s, will check bins for %s" % (enabled_features, ", ".join(checked_features)))
    for arg in args:
        pr("checking binary %s" % arg)
        if is_jemalloc_enabled(enabled_features):
            check_jemalloc(arg)
        if is_sse_enabled(enabled_features):
            check_sse(arg)
        pr("%s %s \033[32menabled\033[0m\n" % (arg, " ".join(checked_features)))

def main():
    argv = sys.argv
    idx = 1
    enabled_features = []
    if argv[idx] == "--features":
        enabled_features = re.split("\s+", argv[idx+1])
        idx += 2
    if argv[idx] == "--check-tests":
        check_tests(enabled_features)
    elif argv[idx] == "--check-release":
        check_release(enabled_features, argv[idx+1:])

main()
