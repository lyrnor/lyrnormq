use rdkafka::{ClientConfig, producer::FutureProducer};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let required = "ssl,sasl,sasl_plain,sasl_scram,lz4,zstd,gzip";
    let _producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:1")
        .set("builtin.features", required)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", "build-probe")
        .set("sasl.password", "non-secret-build-probe")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("compression.type", "lz4")
        .create()?;
    println!("librdkafka {:?}; verified features: {required}", rdkafka::util::get_rdkafka_version());
    Ok(())
}
