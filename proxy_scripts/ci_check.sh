set -uxeo pipefail
if [[ $M == "fmt" ]]; then
    make gen_proxy_ffi
    git status -s
    GIT_STATUS=$(git status -s) && if [[ ${GIT_STATUS} ]]; then echo "Error: found illegal git status"; echo ${GIT_STATUS}; [[ -z ${GIT_STATUS} ]]; fi
    cargo fmt -- --check
elif [[ $M == "testold" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    export RUST_BACKTRACE=full
    export ENABLE_FEATURES="test-engine-kv-rocksdb test-engine-raft-raft-engine"
    echo "Start clippy"
    chmod +x ./proxy_scripts/clippy.sh
    ./proxy_scripts/clippy.sh
    echo "Finish clippy"
    # exit # If we depend TiKV as a Cargo component, the following is not necessary, and can fail.
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_normal
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_bootstrap
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_compact_log
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_early_apply
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_encryption
    # cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_pd_client
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_pending_peers
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_transaction
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_cmd_epoch_checker
    # cargo test --package tests --test failpoints cases::test_disk_full
    cargo test --package tests --test failpoints cases::test_merge
    # cargo test --package tests --test failpoints cases::test_snap
    cargo test --package tests --test failpoints cases::test_import_service
elif [[ $M == "testnew" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    export RUST_BACKTRACE=full
    export ENABLE_FEATURES="test-engine-kv-rocksdb test-engine-raft-raft-engine"
    cargo check --package proxy_server --features="$ENABLE_FEATURES"
    # tests based on mock-engine-store, with compat for new proxy
    cargo test --package proxy_tests --test proxy write
    cargo test --package proxy_tests --test proxy snapshot
    cargo test --package proxy_tests --test proxy normal::store
    cargo test --package proxy_tests --test proxy normal::config
    cargo test --package proxy_tests --test proxy normal::restart
    cargo test --package proxy_tests --test proxy normal::persist
    cargo test --package proxy_tests --test proxy ingest
    cargo test --package proxy_tests --test proxy config
    cargo test --package proxy_tests --test proxy region
    cargo test --package proxy_tests --test proxy flashback
    cargo test --package proxy_tests --test proxy server_cluster_test
    cargo test --package proxy_tests --test proxy fast_add_peer
    cargo test --package proxy_tests --test proxy replica_read -- --test-threads 1
    cargo test --package proxy_tests --test proxy ffi -- --test-threads 1
    cargo test --package proxy_tests --test proxy write --features="proxy_tests/enable-pagestorage"
    # We don't support snapshot test for PS, since it don't support trait Snapshot.
elif [[ $M == "debug" ]]; then
    # export RUSTC_WRAPPER=~/.cargo/bin/sccache
    export ENGINE_LABEL_VALUE=tiflash
    make debug
elif [[ $M == "release" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    make release
fi
