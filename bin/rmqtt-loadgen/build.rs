use std::process::Command;
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
fn main() {
    let revision = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=LOADGEN_GIT_REVISION={revision}");
    for key in ["HEAD", "refs/heads/codex/rust-loadgen"] {
        if let Some(path) = git(&["rev-parse", "--git-path", key]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let dirty = Command::new("git")
        .args([
            "diff",
            "--quiet",
            "HEAD",
            "--",
            ".",
            "../../crates/mqtt-protocol",
            "../../Cargo.toml",
            "../../Cargo.lock",
        ])
        .status()
        .ok()
        .and_then(|s| s.code())
        .map(|c| match c {
            0 => "false",
            1 => "true",
            _ => "unknown",
        })
        .unwrap_or("unknown");
    println!("cargo:rustc-env=LOADGEN_CODE_DIRTY={dirty}");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");
}
