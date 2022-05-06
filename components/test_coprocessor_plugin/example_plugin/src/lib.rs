// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Range;

use coprocessor_plugin_api::*;

#[derive(Default)]
struct ExamplePlugin;

impl CoprocessorPlugin for ExamplePlugin {
    fn on_raw_coprocessor_request(
        &self,
        _ranges: Vec<Range<Key>>,
        _request: RawRequest,
        _storage: &dyn RawStorage,
    ) -> PluginResult<RawResponse> {
        unimplemented!()
    }
}

declare_plugin!(ExamplePlugin::default());
