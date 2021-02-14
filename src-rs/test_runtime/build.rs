use std::{env, path::PathBuf, fs::File, io::{BufRead, BufReader}};

fn project_root() -> PathBuf {
    let cwd = env::current_dir().expect("get current dir");
    cwd.parent().unwrap().parent().unwrap().into()
}

fn grep_config_h(root: &PathBuf, prefix: &str) -> bool {
    let config_h = File::open(root.join("include/config.h")).unwrap();
    for line in BufReader::new(config_h).lines() {
        if line.unwrap().starts_with(prefix) {
            return true
        }
    }
    false
}

fn main() {
    let root = project_root();
    println!("cargo:rustc-link-search={}", root.to_string_lossy());
    println!("cargo:rustc-link-lib=static=octopus");
    println!("cargo:rustc-link-lib=dylib=objc");
    if grep_config_h(&root, "#define HAVE_LIBELF 1") {
        println!("cargo:rustc-link-lib=dylib=elf");
    }
    println!("cargo:rustc-link-search={}/third_party/luajit/src", root.to_string_lossy());
    println!("cargo:rustc-link-lib=static=luajit");
    println!("cargo:rerun-if-changed=build.rs");
}
