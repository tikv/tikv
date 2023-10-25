use std::{future::Future, time::Duration};

use kvproto::{
    errorpb::{Error, StaleCommand},
    metapb::{self, Region},
};
use tikv_util::{box_err, error, info, time::Instant as TiInstant, warn};
use tokio::sync::oneshot;
use txn_types::TimeStamp;

fn epoch_second() -> u64 {
    TimeStamp::physical_now() / 1000
}

enum PrepareState {
    None,
    WaitApply { target: u64, lease_until: u64 },
    TakingSnapshot { lease_until: u64 },
}

impl PrepareState {
    fn start_wait_apply(&mut self, target: u64, lease_dur: Duration) {
        *self = Self::WaitApply {
            target,
            lease_until: epoch_second() + lease_dur.as_secs(),
        }
    }
}

pub struct PrepareSyncer<T> {
    time: TiInstant,
    region_id: u64,
    sender: oneshot::Sender<std::result::Result<T, Error>>,
}

pub struct PrepareSyncResult<T> {
    region: Region,
    result: std::result::Result<T, Error>,
}

impl<T: Send + 'static> PrepareSyncer<T> {
    fn for_region(
        r: &Region,
    ) -> (
        Self,
        impl Future<Output = PrepareSyncResult<T>> + Send + 'static,
    ) {
        let (tx, rx) = oneshot::channel();
        let region_id = r.get_id();
        let this = Self {
            time: TiInstant::now_coarse(),
            region_id,
            sender: tx,
        };
        let region = r.clone();
        let fut = async move {
            let result = match rx.await {
                Ok(item) => item,
                Err(rerr) => Err({
                    let mut err = Error::new();
                    err.set_stale_command(StaleCommand::new());
                    err.set_message(format!("the sender has been dropped for region {region_id} during preparing snapshot backup due to {rerr}"));
                    err
                }),
            };
            PrepareSyncResult { region, result }
        };
        (this, fut)
    }

    fn fulfill(self, t: T) {
        if let Err(_) = self.sender.send(Ok(t)) {
            warn!(
                "PrepareSnapshotBackupSyncer: sender gone, probably aborted but not been checked.";
                "region_id" => self.region_id
            )
        }
    }

    fn fail(self, err: Error) {
        if let Err(_) = self.sender.send(Err(err)) {
            warn!(
                "PrepareSnapshotBackupSyncer: sender gone, probably aborted but not been checked.";
                "region_id" => self.region_id
            )
        }
    }

    fn aborted(&self) -> bool {
        self.sender.is_closed()
    }
}
