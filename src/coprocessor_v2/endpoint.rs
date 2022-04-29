// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::KvFormat;
use coprocessor_plugin_api::*;
use kvproto::kvrpcpb;
use semver::VersionReq;
use std::future::Future;
use std::sync::Arc;

use super::config::Config;
use super::plugin_registry::PluginRegistry;
use super::raw_storage_impl::RawStorageImpl;
use crate::storage::{self, lock_manager::LockManager, Engine, Storage};

#[allow(clippy::large_enum_variant)]
enum CoprocessorError {
    RegionError(kvproto::errorpb::Error),
    Other(String),
}

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct Endpoint {
    plugin_registry: Option<Arc<PluginRegistry>>,
}

impl tikv_util::AssertSend for Endpoint {}

impl Endpoint {
    pub fn new(copr_cfg: &Config) -> Self {
        let plugin_registry =
            copr_cfg
                .coprocessor_plugin_directory
                .as_ref()
                .map(|plugin_directory| {
                    let mut plugin_registry = PluginRegistry::new();

                    if let Err(err) = plugin_registry.start_hot_reloading(plugin_directory) {
                        warn!(
                            "unable to start hot-reloading for coprocessor plugins.";
                            "coprocessor_directory" => plugin_directory.display(),
                            "error" => ?err
                        );
                    }

                    Arc::new(plugin_registry)
                });

        Self { plugin_registry }
    }

    /// Handles a request to the coprocessor framework.
    ///
    /// Each request is dispatched to the corresponding coprocessor plugin based on it's `copr_name`
    /// field. A plugin with a matching name must be loaded by TiKV, otherwise an error is returned.
    #[inline]
    pub fn handle_request<E: Engine, L: LockManager, F: KvFormat>(
        &self,
        storage: &Storage<E, L, F>,
        req: kvrpcpb::RawCoprocessorRequest,
    ) -> impl Future<Output = kvrpcpb::RawCoprocessorResponse> {
        let mut response = kvrpcpb::RawCoprocessorResponse::default();

        let coprocessor_result = self.handle_request_impl(storage, req);

        match coprocessor_result {
            Ok(data) => response.set_data(data),
            Err(CoprocessorError::RegionError(region_err)) => response.set_region_error(region_err),
            Err(CoprocessorError::Other(o)) => response.set_error(o),
        }

        std::future::ready(response)
    }

    #[inline]
    fn handle_request_impl<E: Engine, L: LockManager, F: KvFormat>(
        &self,
        storage: &Storage<E, L, F>,
        mut req: kvrpcpb::RawCoprocessorRequest,
    ) -> Result<RawResponse, CoprocessorError> {
        let plugin_registry = self
            .plugin_registry
            .as_ref()
            .ok_or_else(|| CoprocessorError::Other("Coprocessor plugin is disabled!".to_owned()))?;

        let plugin = plugin_registry.get_plugin(&req.copr_name).ok_or_else(|| {
            CoprocessorError::Other(format!(
                "No registered coprocessor with name '{}'",
                req.copr_name
            ))
        })?;

        // Check whether the found plugin satisfies the version constraint.
        let version_req = VersionReq::parse(&req.copr_version_req)
            .map_err(|e| CoprocessorError::Other(format!("{}", e)))?;
        let plugin_version = plugin.version();

        if !version_req.matches(plugin_version) {
            return Err(CoprocessorError::Other(format!(
                "The plugin '{}' with version '{}' does not satisfy the version constraint '{}'",
                plugin.name(),
                plugin_version,
                version_req,
            )));
        }

        let raw_storage_api = RawStorageImpl::new(req.take_context(), storage);
        let ranges = req
            .take_ranges()
            .into_iter()
            .map(|range| range.start_key..range.end_key)
            .collect();

        let plugin_result = plugin.on_raw_coprocessor_request(ranges, req.data, &raw_storage_api);

        plugin_result.map_err(|err| {
            if let Some(region_err) = extract_region_error(&err) {
                CoprocessorError::RegionError(region_err)
            } else {
                CoprocessorError::Other(format!("{}", err))
            }
        })
    }
}

fn extract_region_error(error: &PluginError) -> Option<kvproto::errorpb::Error> {
    match error {
        PluginError::Other(_, other_err) => other_err
            .downcast_ref::<storage::Result<()>>()
            .and_then(|e| storage::errors::extract_region_error::<()>(e)),
        _ => None,
    }
}
