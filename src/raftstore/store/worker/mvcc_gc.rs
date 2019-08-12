use std::fmt::{self, Display};

use tikv_util::worker::Runnable;

#[derive(Clone, Debug)]
pub struct MvccGcTask {
    safe_point: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

impl Display for MvccGcTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MvccGc")
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

pub struct MvccGcRunner;

impl MvccGcRunner {
    pub fn new() -> MvccGcRunner {
        MvccGcRunner {}
    }
}

impl Runnable<MvccGcTask> for MvccGcRunner {
    fn run(&mut self, _task: MvccGcTask) {
        unimplemented!();
    }
}
