use coprocessor_plugin_api::*;

#[derive(Default)]
struct ExamplePlugin;

impl CoprocessorPlugin for ExamplePlugin {
    fn create() -> Self
    where
        Self: Sized,
    {
        ExamplePlugin::default()
    }

    fn name(&self) -> &'static str {
        "example-plugin"
    }

    fn on_raw_coprocessor_request(
        &self,
        _region: &Region,
        _request: &RawRequest,
        _storage: &dyn RawStorage,
    ) -> Result<RawResponse, Box<dyn std::error::Error>> {
        todo!()
    }
}

declare_plugin!(ExamplePlugin);
