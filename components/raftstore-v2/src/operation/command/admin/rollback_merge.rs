// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! The rollback of `PrepareMerge` command.
//!
//! # Propose
//!
//! A `RollbackMerge` command is proposed only when peers that have such
//! intention have reached a quorum. The intention is recorded by leader in
//! `Peer::want_rollback_merge_peers`.
