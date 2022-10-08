if [[ $M == "fmt" ]]; then
    make gen_proxy_ffi
    GIT_STATUS=$(git status -s) && if [[ ${GIT_STATUS} ]]; then echo "Error: found illegal git status"; echo ${GIT_STATUS}; [[ -z ${GIT_STATUS} ]]; fi
    cargo fmt -- --check >/dev/null
elif [[ $M == "testold" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    export RUST_BACKTRACE=full
    cargo check
    cargo test --package tests --test failpoints cases::test_normal
    cargo test --package tests --test failpoints cases::test_bootstrap
    cargo test --package tests --test failpoints cases::test_compact_log
    cargo test --package tests --test failpoints cases::test_early_apply
    cargo test --package tests --test failpoints cases::test_encryption
    cargo test --package tests --test failpoints cases::test_pd_client
    cargo test --package tests --test failpoints cases::test_pending_peers
    cargo test --package tests --test failpoints cases::test_transaction
    cargo test --package tests --test failpoints cases::test_cmd_epoch_checker
    cargo test --package tests --test failpoints cases::test_disk_full
    cargo test --package tests --test failpoints cases::test_snap
    cargo test --package tests --test failpoints cases::test_merge
    cargo test --package tests --test failpoints cases::test_import_service
    cargo test --package tests --test failpoints cases::test_proxy_replica_read
elif [[ $M == "testnew" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    export RUST_BACKTRACE=full
    # tests based on new-mock-engine-store, with compat for new proxy
    cargo test --package tests --test proxy normal::store
    cargo test --package tests --test proxy normal::region
    cargo test --package tests --test proxy normal::config
    cargo test --package tests --test proxy normal::write
    cargo test --package tests --test proxy normal::ingest
    cargo test --package tests --test proxy normal::snapshot
    cargo test --package tests --test proxy normal::restart
    cargo test --package tests --test proxy normal::persist
    # tests based on new-mock-engine-store, for some tests not available for new proxy
    cargo test --package tests --test proxy proxy
elif [[ $M == "debug" ]]; then
    # export RUSTC_WRAPPER=~/.cargo/bin/sccache
    export ENGINE_LABEL_VALUE=tiflash
    make debug
elif [[ $M == "release" ]]; then
    export ENGINE_LABEL_VALUE=tiflash
    make release
fi
