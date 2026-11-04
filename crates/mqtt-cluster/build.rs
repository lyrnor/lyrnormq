fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .bytes(".rmqtt.cluster.v1.Envelope.data")
        .compile_protos(&["proto/cluster.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/cluster.proto");
    Ok(())
}
