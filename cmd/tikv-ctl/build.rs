// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// Pay attention to synchronize this file with cmd/tikv-server/build.rs.

fn main() {
    println!(
        "cargo:rustc-env=TIKV_BUILD_TIME={}",
        time::now_utc().strftime("%Y-%m-%d %H:%M:%S").unwrap()
    );
}
