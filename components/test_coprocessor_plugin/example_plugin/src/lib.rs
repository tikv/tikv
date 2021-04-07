// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use coprocessor_plugin_api::*;

#[derive(Default)]
struct ExamplePlugin;

impl CoprocessorPlugin for ExamplePlugin {
    fn name(&self) -> &'static str {
        "example-plugin"
    }

    fn on_raw_coprocessor_request(
        &self,
        _region: &Region,
        _request: &RawRequest,
        _storage: &dyn RawStorage,
    ) -> Result<RawResponse, PluginError> {
        unimplemented!()
    }
}

declare_plugin!(ExamplePlugin::default());
