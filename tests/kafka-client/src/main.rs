//! Bounded independent real-Kafka admin/consumer helper, never production data.
use rdkafka::{
    ClientConfig, Message, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer},
    message::Headers,
    producer::{FutureProducer, FutureRecord},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error,
    io,
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn invalid(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, s)
}
fn config(path: &str) -> Result<ClientConfig> {
    let bytes = std::fs::read(path)?;
    if bytes.len() > 65536 {
        return Err(invalid("oversized helper configuration").into());
    }
    let values: BTreeMap<String, String> = serde_json::from_slice(&bytes)?;
    let mut c = ClientConfig::new();
    c.set_log_level(RDKafkaLogLevel::Critical);
    for (k, v) in values {
        c.set(k, v);
    }
    c.set("socket.timeout.ms", "5000")
        .set("socket.connection.setup.timeout.ms", "5000");
    Ok(c)
}
#[tokio::main]
async fn main() -> Result<()> {
    let a: Vec<_> = std::env::args().skip(1).collect();
    if a.len() < 3 {
        return Err(invalid(
            "usage: create|consume|partition CONFIG TOPIC [COUNT|PARTITIONS|KEY] [TIMEOUT_MS]",
        )
        .into());
    }
    let mut c = config(&a[1])?;
    match a[0].as_str() {
        "create" => {
            let partitions = a.get(3).map(|v| v.parse::<i32>()).transpose()?.unwrap_or(3);
            if !(1..=32).contains(&partitions) {
                return Err(invalid("partition bound").into());
            }
            let admin: AdminClient<DefaultClientContext> = c.create()?;
            let result = admin
                .create_topics(
                    &[NewTopic::new(&a[2], partitions, TopicReplication::Fixed(1))],
                    &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
                )
                .await?;
            for result in result {
                result.map_err(|_| invalid("topic creation failed"))?;
            }
            println!("{}", json!({"created":a[2],"partitions":partitions}));
        }
        "partition" => {
            let key = a.get(3).ok_or_else(|| invalid("missing partition key"))?;
            if key.len() > 1024 {
                return Err(invalid("partition oracle key bound").into());
            }
            c.set("partitioner", "murmur2")
                .set("acks", "all")
                .set("enable.idempotence", "true")
                .set("message.timeout.ms", "5000")
                .set("queue.buffering.max.messages", "16")
                .set("queue.buffering.max.kbytes", "1024");
            let producer: FutureProducer = c.create()?;
            let delivery = producer
                .send(
                    FutureRecord::to(&a[2]).key(key.as_str()).payload(b"{}"),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|_| invalid("partition oracle delivery failed"))?;
            println!(
                "{}",
                json!({"partition":delivery.partition,"offset":delivery.offset})
            );
        }
        "consume" => {
            let count = a
                .get(3)
                .ok_or_else(|| invalid("missing count"))?
                .parse::<usize>()?;
            let timeout = a
                .get(4)
                .map(|v| v.parse::<u64>())
                .transpose()?
                .unwrap_or(5000);
            if count > 10000 || !(100..=60000).contains(&timeout) {
                return Err(invalid("read bound").into());
            }
            c.set("group.id", format!("lyrnormq-quality-{}", std::process::id()))
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .set("enable.partition.eof", "true")
                .set("queued.max.messages.kbytes", "8192")
                .set("fetch.message.max.bytes", "2097152")
                .set("fetch.max.bytes", "4194304");
            let consumer: BaseConsumer = c.create()?;
            let metadata = consumer.fetch_metadata(Some(&a[2]), Duration::from_secs(8))?;
            let topic = metadata
                .topics()
                .iter()
                .find(|t| t.name() == a[2])
                .ok_or_else(|| invalid("topic metadata missing"))?;
            if topic.partitions().len() > 32 {
                return Err(invalid("metadata bound").into());
            }
            let mut assignment = TopicPartitionList::new();
            for partition in topic.partitions() {
                assignment.add_partition_offset(
                    &a[2],
                    partition.id(),
                    rdkafka::Offset::Beginning,
                )?;
            }
            consumer.assign(&assignment)?;
            let deadline = Instant::now() + Duration::from_millis(timeout);
            let mut records: Vec<Value> = Vec::new();
            let mut received_bytes = 0usize;
            while records.len() < count && Instant::now() < deadline {
                match consumer.poll(Duration::from_millis(50)) {
                    Some(Ok(message)) => {
                        let mut headers = BTreeMap::new();
                        if let Some(h) = message.headers() {
                            for i in 0..h.count() {
                                let h = h.get(i);
                                headers.insert(
                                    h.key.to_owned(),
                                    String::from_utf8(h.value.unwrap_or_default().to_vec())?,
                                );
                            }
                        }
                        let key = message
                            .key()
                            .map(|v| String::from_utf8(v.to_vec()))
                            .transpose()?;
                        let raw = message.payload().ok_or_else(|| invalid("null value"))?;
                        received_bytes = received_bytes.saturating_add(raw.len());
                        if received_bytes > 16 * 1024 * 1024 {
                            return Err(invalid("aggregate payload bound").into());
                        }
                        let value: Value = serde_json::from_slice(raw)?;
                        records.push(json!({"key":key,"value":value,"headers":headers,"partition":message.partition(),"offset":message.offset(),"timestamp_ms":message.timestamp().to_millis()}));
                    }
                    Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) | None => {}
                    Some(Err(_)) => return Err(invalid("Kafka consumption failed").into()),
                }
            }
            println!("{}", json!({"records":records,"count":records.len()}));
        }
        _ => return Err(invalid("invalid operation").into()),
    }
    Ok(())
}
