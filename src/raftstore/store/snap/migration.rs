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
use super::*;

pub(super) fn parse_old_snap_name(name: &str) -> Option<(bool, SnapKey)> {
    let pattern = Regex::new("(gen|rev)_([0-9]+)_([0-9]+)_([0-9]+)").unwrap();
    let caps = pattern.captures(name)?;
    let for_send = caps.at(1)? == SNAP_GEN_PREFIX;
    let region_id = caps.at(2)?.parse().ok()?;
    let term = caps.at(3)?.parse().ok()?;
    let idx = caps.at(4)?.parse().ok()?;
    Some((for_send, SnapKey::new(region_id, term, idx)))
}

pub(super) fn gen_old_cf_file_path(dir: &str, for_send: bool, key: SnapKey, cf: &str) -> PathBuf {
    let dir = PathBuf::from(dir);
    let file_name = if for_send {
        // gen_1_2_2_lock.sst
        format!("{}_{}_{}{}", SNAP_GEN_PREFIX, key, cf, SST_FILE_SUFFIX)
    } else {
        // rev_1_2_2_lock.sst
        format!("{}_{}_{}{}", SNAP_REV_PREFIX, key, cf, SST_FILE_SUFFIX)
    };
    dir.join(&file_name)
}
