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

use kvproto::kvrpcpb::*;

use pd::RegionInfo;

use super::Result;

pub trait ImportClient: Send + Sync + Clone + 'static {
    fn get_region(&self, _: &[u8]) -> Result<RegionInfo>;

    fn split_region(&self, _: &RegionInfo, _: &[u8]) -> Result<SplitRegionResponse>;

    fn scatter_region(&self, _: &RegionInfo) -> Result<()>;
}
