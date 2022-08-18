// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use slog::Logger;

use super::Peer;
use crate::tablet::CachedTablet;

/// Apply applies all the committed commands to kv db.
pub struct Apply<EK: KvEngine> {
    tablet: CachedTablet<EK>,
    logger: Logger,
}

impl<EK: KvEngine> Apply<EK> {
    #[inline]
    pub fn new<ER: RaftEngine>(peer: &Peer<EK, ER>) -> Self {
        Apply {
            tablet: peer.tablet().clone(),
            logger: peer.logger.clone(),
        }
    }
}
