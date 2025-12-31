# Verify that individual crates build with various feature combinations
#
# re: https://github.com/tikv/tikv/issues/9406
#
# TiKV has several compile time features that are threaded through the
# dependency tree. It is common that the features are not configured correctly,
# causing individual crates to not build on their own. This script goes through
# each individual crate and runs `cargo check -p` and `cargo test -p --no-run`
# on them, with multiple feature combinations.
#
# In particular it verifies that these configurations work:
#
# - The default
# - With `test-engines-panic`
#
# Requires Python 3.6+

import os
import subprocess
import sys

components = [f for f in os.listdir("components") if os.path.isdir(f"components/{f}")]

other_crates = [
    ("cmd", "cmd"),
    ("tests", "tests"),
    ("tikv", "./"),
]

components_with_dirs = [(x, f"components/{x}") for x in components]
crates = components_with_dirs + other_crates

errors = []

def cargo_check_default():
    cargo_run_default("check", [])

def cargo_test_default():
    cargo_run_default("test", ["--no-run"])

def cargo_run_default(cmd, extra_args):
    for (crate, _) in crates:
        args = ["cargo", cmd, "-p", crate]
        args += extra_args
        run_and_collect_errors(args)

def cargo_check_test_engines(engines):
    cargo_run_test_engines("check", [], engines)

def cargo_test_test_engines(engines):
    cargo_run_test_engines("test", ["--no-run"], engines)

def cargo_run_test_engines(cmd, extra_args, engines):
    for (crate, path) in crates:
        has_test_engine_features = get_features(path)

        if not has_test_engine_features:
            continue

        args = ["cargo", cmd, "-p", crate, "--no-default-features"]
        args += extra_args
        if has_test_engine_features:
            args += ["--features", f"test-engines-{engines}"]

        run_and_collect_errors(args)

def cargo_check_test_engines_ext(kv_engine, raft_engine):
    cargo_run_test_engines_ext("check", [], kv_engine, raft_engine)

def cargo_test_test_engines_ext(kv_engine, raft_engine):
    cargo_run_test_engines_ext("test", ["--no-run"], kv_engine, raft_engine)

def cargo_run_test_engines_ext(cmd, extra_args, kv_engine, raft_engine):
    for (crate, path) in crates:
        has_test_engine_features = get_features(path)

        if not has_test_engine_features:
            continue

        args = ["cargo", cmd, "-p", crate, "--no-default-features"]
        args += extra_args
        if has_test_engine_features:
            args += ["--features", f"test-engine-kv-{kv_engine}"]
            args += ["--features", f"test-engine-raft-{raft_engine}"]

        run_and_collect_errors(args)

def run_and_collect_errors(args):
    global errors
    joined_args = " ".join(args)
    print(f"running `{joined_args}`")
    res = subprocess.run(args)
    if res.returncode != 0:
        errors += [joined_args]

def get_features(path):
    path = f"{path}/Cargo.toml"
    f = open(path)
    s = f.read()
    f.close()

    has_test_engine_features = "test-engines-rocksdb" in s

    return has_test_engine_features

print()

cargo_check_default()
cargo_check_test_engines("panic")
cargo_check_test_engines("rocksdb")
cargo_check_test_engines_ext("rocksdb", "raft-engine")
cargo_test_default()
cargo_test_test_engines("panic")
cargo_test_test_engines("rocksdb")
cargo_test_test_engines_ext("rocksdb", "raft-engine")

if len(errors) == 0:
    sys.exit(0)

print()
print("errors:")
    
for error in errors:
    print(f"    {error}")

print()

sys.exit(1)
