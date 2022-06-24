// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

fn link_cpp(tool: &cc::Tool) {
    let stdlib = if tool.is_like_gnu() {
        "libstdc++.a"
    } else if tool.is_like_clang() {
        "libc++.a"
    } else {
        // Don't link to c++ statically on windows.
        return;
    };
    link_sys_lib(stdlib, tool)
}

fn link_sys_lib(lib: &str, tool: &cc::Tool) {
    let output = tool
        .to_command()
        .arg("--print-file-name")
        .arg(lib)
        .output()
        .unwrap();
    if !output.status.success() || output.stdout.is_empty() {
        // fallback to dynamically
        return;
    }
    let path = match std::str::from_utf8(&output.stdout) {
        Ok(path) => std::path::PathBuf::from(path),
        Err(_) => return,
    };
    if !path.is_absolute() {
        return;
    }
    // remove lib prefix and .a postfix.
    let libname = &lib[3..lib.len() - 2];
    println!("cargo:rustc-link-lib=static:+whole-archive={}", &libname);
    println!(
        "cargo:rustc-link-search=native={}",
        path.parent().unwrap().display()
    );
}

fn main() {
    println!(
        "cargo:rustc-env=TIKV_BUILD_TIME={}",
        time::now_utc().strftime("%Y-%m-%d %H:%M:%S").unwrap()
    );

    let tool = cc::Build::default().get_compiler();
    link_cpp(&tool);
    // Maybe we can link more sys libraries.
}
