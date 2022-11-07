set -uxeo pipefail
if [[ $M == "fmt" ]]; then
    make gen_proxy_ffi
    GIT_STATUS=$(git status -s) && if [[ ${GIT_STATUS} ]]; then echo "Error: found illegal git status"; echo ${GIT_STATUS}; [[ -z ${GIT_STATUS} ]]; fi
    cargo fmt -- --check >/dev/null
elif [[ $M == "testold" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    export RUST_BACKTRACE=full
    export ENABLE_FEATURES="test-engine-kv-rocksdb test-engine-raft-raft-engine"
	rustup component add clippy
    cargo clippy --features "$ENABLE_FEATURES" --package engine_store_ffi --no-deps -- -Dwarnings -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
    cargo clippy --features "$ENABLE_FEATURES" --package proxy_tests  --no-deps -- -Dwarnings -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
    cargo clippy --features "$ENABLE_FEATURES" --package proxy_server  --no-deps -- -Dwarnings -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq
    cargo clippy --features "$ENABLE_FEATURES" --package new-mock-engine-store  --no-deps -- -Dwarnings -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq -A clippy::redundant_clone -A clippy::too_many_arguments
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_normal
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_bootstrap
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_compact_log
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_early_apply
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_encryption
    cargo test --features "$ENABLE_FEATURES" --package tests --test failpoints cases::test_pd_client
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
    # tests based on new-mock-engine-store, with compat for new proxy
    cargo test --package proxy_tests --test proxy normal::store
    cargo test --package proxy_tests --test proxy normal::region
    cargo test --package proxy_tests --test proxy normal::config
    cargo test --package proxy_tests --test proxy normal::write
    cargo test --package proxy_tests --test proxy normal::ingest
    cargo test --package proxy_tests --test proxy normal::snapshot
    cargo test --package proxy_tests --test proxy normal::restart
    cargo test --package proxy_tests --test proxy normal::persist
    cargo test --package proxy_tests --test proxy config
    cargo test --package proxy_tests --test proxy flashback
    cargo test --package proxy_tests --test proxy server_cluster_test
    # tests based on new-mock-engine-store, for some tests not available for new proxy
    cargo test --package proxy_tests --test proxy proxy
elif [[ $M == "debug" ]]; then
    # export RUSTC_WRAPPER=~/.cargo/bin/sccache
    export ENGINE_LABEL_VALUE=tiflash
    make debug
elif [[ $M == "release" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    make release
fi
