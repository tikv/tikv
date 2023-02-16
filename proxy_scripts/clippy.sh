set -uxeo pipefail
export ENGINE_LABEL_VALUE=tiflash
export RUST_BACKTRACE=full
export ENABLE_FEATURES="test-engine-kv-rocksdb test-engine-raft-raft-engine"
export ENABLE_FEATURES_PS="test-engine-kv-rocksdb test-engine-raft-raft-engine enable-pagestorage"
rustup component add clippy
cargo clippy --features "$ENABLE_FEATURES" --package proxy_ffi --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
cargo clippy --features "$ENABLE_FEATURES" --package engine_store_ffi --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
cargo clippy --features "$ENABLE_FEATURES" --package proxy_tests  --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
cargo clippy --features "$ENABLE_FEATURES" --package proxy_server  --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq
cargo clippy --features "$ENABLE_FEATURES" --package mock-engine-store  --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq -A clippy::redundant_clone -A clippy::too_many_arguments
cargo clippy --features "$ENABLE_FEATURES" --package engine_tiflash  --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq -A clippy::redundant_clone -A clippy::too_many_arguments
cargo clippy --features "$ENABLE_FEATURES_PS" --package engine_tiflash  --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq -A clippy::redundant_clone -A clippy::too_many_arguments