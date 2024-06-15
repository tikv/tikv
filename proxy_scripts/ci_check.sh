set -uxeo pipefail
cat /etc/issue
cat /proc/version
echo "LD_LIBRARY_PATH=", ${LD_LIBRARY_PATH:-nil}
echo "PATH=", $PATH

if [[ $M == "fmt" ]]; then
    pwd
    git rev-parse --show-toplevel
    make gen_proxy_ffi
    git status -s .
    GIT_STATUS=$(git status -s .) && if [[ ${GIT_STATUS} ]]; then echo "Error: found illegal git status"; echo ${GIT_STATUS}; [[ -z ${GIT_STATUS} ]]; fi
    cargo fmt -- --check
elif [[ $M == "testold" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    export RUST_BACKTRACE=full
    export ENABLE_FEATURES="test-engine-kv-rocksdb test-engine-raft-raft-engine"
    echo "Start clippy"
    chmod +x ./proxy_scripts/clippy.sh
    ./proxy_scripts/clippy.sh
    echo "Finish clippy"
    chmod +x ./proxy_scripts/tikv-code-consistency.sh
    ./proxy_scripts/tikv-code-consistency.sh
    echo "Finish tikv code consistency"
    exit # If we depend TiKV as a Cargo component, the following is not necessary, and can fail.
    # TODO we have to let tests support openssl-vendored.
    yum install openssl openssl-devel -y
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
    # cargo test --package tests --test failpoints cases::test_merge -- --skip test_node_merge_restart --skip test_node_merge_catch_up_logs_no_need
    # cargo test --package tests --test failpoints cases::test_snap
    cargo test --package tests --test failpoints cases::test_import_service
elif [[ $M == "testnew" ]]; then
    chmod +x ./proxy_scripts/make_env.sh
    ./proxy_scripts/make_env.sh
    export ENGINE_LABEL_VALUE=tiflash
    export RUST_BACKTRACE=full
    export ENABLE_FEATURES="test-engine-kv-rocksdb test-engine-raft-raft-engine openssl-vendored"
    cargo check --package proxy_server --features="$ENABLE_FEATURES"
    # tests based on mock-engine-store, with compat for new proxy
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::jemalloc --features="jemalloc"
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::write
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::snapshot
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::config
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::normal::restart
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::normal::persist
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::ingest
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::engine
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::store
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::region
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::flashback
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy v2_compat::cluster_raftstore_ver
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy v2_compat::tablet_snapshot
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy v2_compat::simple_write
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy v1_specific::region_ext
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy v1_specific::flashback
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::server_cluster_test -- --test-threads 1
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::fast_add_peer
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::replica_read -- --test-threads 1
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::ffi -- --test-threads 1
    cargo test --package proxy_tests --features="$ENABLE_FEATURES" --test proxy shared::write --features="proxy_tests/enable-pagestorage"
    # We don't support snapshot test for PS, since it don't support trait Snapshot.
elif [[ $M == "debug" ]]; then
    # export RUSTC_WRAPPER=~/.cargo/bin/sccache
    export ENGINE_LABEL_VALUE=tiflash
    make debug
elif [[ $M == "release" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    make release
fi
   