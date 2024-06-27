// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

#[macro_export]
macro_rules! send_rpc_response {
    ($res:ident, $sink:ident, $label:ident, $timer:ident) => {{
        let res = match $res {
            Ok(resp) => {
                IMPORT_RPC_DURATION
                    .with_label_values(&[$label, "ok"])
                    .observe($timer.saturating_elapsed_secs());
                $sink.success(resp)
            }
            Err(e) => {
                IMPORT_RPC_DURATION
                    .with_label_values(&[$label, "error"])
                    .observe($timer.saturating_elapsed_secs());
                error_inc($label, &e);
                $sink.fail(make_rpc_error(e))
            }
        };
        let _ = res.map_err(|e| warn!("send rpc response"; "err" => %e)).await;
    }};
}
