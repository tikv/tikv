// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// Reference: https://github.com/larksuite/perf-monitor-rs/blob/master/build.rs
#![allow(clippy::all, clippy::restriction, clippy::style, clippy::perf)]
use std::{convert::AsRef, env, ffi::OsStr, path::Path, process::Command};

/// Using `build.rs` to generate bindings to eliminate the difference
/// among the different target.
///
/// Following command generate the same output,
/// which makes it easy to glance the bindings when coding.
///
/// ```shell ignore
/// echo "" > /tmp/bindings.h
/// echo "#include <mach/thread_info.h>"  >> /tmp/bindings.h
/// echo "#include <mach/thread_act.h>"  >> /tmp/bindings.h
/// echo "#include <mach/mach_init.h>"  >> /tmp/bindings.h
/// echo "#include <mach/kern_return.h>"  >> /tmp/bindings.h
/// echo "#include <mach/task.h>"  >> /tmp/bindings.h
/// echo "#include <mach/vm_map.h>"  >> /tmp/bindings.h
/// echo "#include <mach/host_info.h>"  >> /tmp/bindings.h
/// echo "#include <mach/mach_host.h>"  >> /tmp/bindings.h
/// echo "#include <pthread/pthread.h>"  >> /tmp/bindings.h
/// echo "#include <mach/mach_traps.h>"  >> /tmp/bindings.h
///
/// bindgen \
/// --with-derive-default \
/// --with-derive-eq \
/// --with-derive-ord \
/// --no-layout-tests \
/// --whitelist-var THREAD_BASIC_INFO \
/// --whitelist-var KERN_SUCCESS \
/// --whitelist-var HOST_BASIC_INFO \
/// --whitelist-var mach_task_self_ \
/// --whitelist-var TH_USAGE_SCALE \
/// --whitelist-type thread_basic_info \
/// --whitelist-type host_basic_info \
/// --whitelist-function thread_info \
/// --whitelist-function mach_thread_self \
/// --whitelist-function task_threads \
/// --whitelist-function vm_deallocate \
/// --whitelist-function host_info \
/// --whitelist-function mach_host_self \
/// --whitelist-function pthread_from_mach_thread_np \
/// --whitelist-function pthread_getname_np \
/// --whitelist-function task_for_pid \
/// /tmp/bindings.h > /tmp/bindings.rs
/// ```

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS");
    if target_os != Ok("macos".into()) && target_os != Ok("ios".into()) {
        return;
    }

    let target_arch = env::var("CARGO_CFG_TARGET_ARCH");

    fn build_include_path(sdk: impl AsRef<OsStr>) -> String {
        let output = Command::new("xcrun")
            .arg("--sdk")
            .arg(sdk)
            .arg("--show-sdk-path")
            .output()
            .expect("failed to run xcrun");
        let sdk_path = String::from_utf8(output.stdout.clone()).expect("valid path");
        format!("{}/usr/include", sdk_path.trim())
    }

    let mut include_path = String::new();

    if target_os == Ok("ios".into()) && target_arch == Ok("aarch64".into()) {
        env::set_var("TARGET", "arm64-apple-ios");
        include_path = build_include_path("iphoneos");
    }

    if target_os == Ok("ios".into()) && target_arch == Ok("x86_64".into()) {
        env::set_var("TARGET", "x86_64-apple-ios");
        include_path = build_include_path("iphonesimulator");
    }

    if target_os == Ok("macos".into()) {
        if target_arch == Ok("x86_64".into()) {
            env::set_var("TARGET", "x86_64-apple-darwin");
            include_path = build_include_path("macosx");
        }
        if target_arch == Ok("aarch64".into()) {
            env::set_var("TARGET", "aarch64-apple-darwin");
            include_path = build_include_path("macosx");
        }
    }

    let outdir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let outfile = Path::new(&outdir).join("macos_ios_binding.rs");

    bindgen::Builder::default()
        .derive_default(true)
        .derive_eq(true)
        .derive_ord(true)
        .layout_tests(false)
        // clang args
        .clang_arg("-I")
        .clang_arg(include_path)
        // headers
        .header_contents(
            "ios_macos.h",
            [
                "#include <mach/processor_info.h>",
                "#include <mach/machine.h>",
                "#include <mach/thread_info.h>",
                "#include <mach/thread_act.h>",
                "#include <mach/mach_init.h>",
                "#include <mach/kern_return.h>",
                "#include <mach/task.h>",
                "#include <mach/vm_map.h>",
                "#include <mach/host_info.h>",
                "#include <mach/mach_host.h>",
                "#include <pthread/pthread.h>",
                "#include <mach/mach_traps.h>",
                "#include <mach/task_info.h>",
                "#include <sys/resource.h>",
                "#include <malloc/malloc.h>",
            ]
                .join("\n")
                .as_str(),
        )
        // var
        .allowlist_var("THREAD_BASIC_INFO")
        .allowlist_var("KERN_SUCCESS")
        .allowlist_var("HOST_BASIC_INFO")
        .allowlist_var("mach_task_self_")
        .allowlist_var("TH_USAGE_SCALE")
        .allowlist_var("PROCESSOR_CPU_LOAD_INFO")
        .allowlist_var("CPU_STATE_USER")
        .allowlist_var("CPU_STATE_SYSTEM")
        .allowlist_var("CPU_STATE_IDLE")
        .allowlist_var("CPU_STATE_NICE")
        // type
        .allowlist_type("thread_basic_info")
        .allowlist_type("host_basic_info")
        .allowlist_type("task_vm_info")
        .allowlist_type("rusage_info_v2")
        .allowlist_type("malloc_zone_t")
        .allowlist_type("host_t")
        .allowlist_type("vm_offset_t")
        .allowlist_type("vm_address_t")
        .allowlist_type("processor_flavor_t")
        .allowlist_type("natural_t")
        .allowlist_type("processor_info_array_t")
        .allowlist_type("processor_cpu_load_info_data_t")
        .allowlist_type("mach_msg_type_number_t")
        .allowlist_type("kern_return_t")
        // function
        .allowlist_function("thread_info")
        .allowlist_function("mach_thread_self")
        .allowlist_function("task_threads")
        .allowlist_function("vm_deallocate")
        .allowlist_function("host_info")
        .allowlist_function("host_processor_info")
        .allowlist_function("mach_host_self")
        .allowlist_function("pthread_from_mach_thread_np")
        .allowlist_function("pthread_getname_np")
        .allowlist_function("task_for_pid")
        .allowlist_function("malloc_get_all_zones")
        .allowlist_function("malloc_default_zone")
        // generate
        .generate()
        .expect("generate binding failed")
        .write_to_file(Path::new(&outfile))
        .expect("write to file failed")
}
