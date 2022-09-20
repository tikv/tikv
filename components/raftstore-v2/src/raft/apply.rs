// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use slog::Logger;

use super::Peer;
use crate::tablet::CachedTablet;

/// Apply applies all the committed commands to kv db.
pub struct Apply<EK: KvEngine> {
    pub(crate) tablet: CachedTablet<EK>,
    pub(crate) logger: Logger,
}

impl<EK: KvEngine> Apply<EK> {
    #[inline]
    pub fn new<ER: RaftEngine>(peer: &Peer<EK, ER>) -> Self {
        Apply {
            tablet: peer.tablet().clone(),
            logger: peer.logger.clone(),
        }
    }

    pub fn tablet(&mut self) -> Option<EK> {
        self.tablet.latest().cloned()
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    // only for test
    pub fn mock(tablet: CachedTablet<EK>, logger: Logger) -> Self {
        Apply { tablet, logger }
    }
}
