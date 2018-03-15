// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::sync::Arc;

use uuid::Uuid;
use kvproto::importpb::SSTMeta;

use import::SSTImporter;
use util::worker::Runnable;

pub enum Task {
    DeleteSST { sst: SSTMeta },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Task::DeleteSST { ref sst } => match Uuid::from_bytes(sst.get_uuid()) {
                Ok(uuid) => write!(f, "Delete SST {}", uuid),
                Err(e) => write!(f, "Delete SST {:?}", e),
            },
        }
    }
}

pub struct Runner {
    importer: Arc<SSTImporter>,
}

impl Runner {
    pub fn new(importer: Arc<SSTImporter>) -> Runner {
        Runner { importer: importer }
    }

    fn handle_delete_sst(&self, sst: SSTMeta) {
        let _ = self.importer.delete(&sst);
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::DeleteSST { sst } => {
                self.handle_delete_sst(sst);
            }
        }
    }
}
