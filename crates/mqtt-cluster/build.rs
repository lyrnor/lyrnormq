fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .bytes(".rmqtt.cluster.v1.Envelope.data")
        .compile_protos(&["proto/cluster.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/cluster.proto");
    link_libstdcpp();
    Ok(())
}

fn link_libstdcpp() {
    let target = std::env::var("TARGET").unwrap_or_default();
    if !target.ends_with("linux-gnu") {
        return;
    }
    let Ok(output) = std::process::Command::new("g++")
        .arg("-print-file-name=libstdc++.so")
        .output()
    else {
        return;
    };
    let path = String::from_utf8_lossy(&output.stdout);
    let path = path.trim();
    let Some(dir) = std::path::Path::new(path).parent() else {
        return;
    };
    if dir.as_os_str().is_empty() || path == "libstdc++.so" {
        return;
    }
    println!("cargo:rustc-link-search=native={}", dir.display());
}
