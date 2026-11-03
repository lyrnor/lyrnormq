//! Bounded, absolute-rate MQTT5 connection ramp; independent of message timing.
#[path = "connections/options.rs"]
mod options;
use options::Options;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
};

fn default_prefix(stamp: u64, process: u32) -> String {
    format!("c{stamp:010}p{process}")
}

fn failure(message: &str) -> io::Error {
    io::Error::other(message)
}
fn connect_packet(client: &str) -> io::Result<Vec<u8>> {
    let id = u16::try_from(client.len()).map_err(|_| failure("client id too long"))?;
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 0, 0];
    body.extend(id.to_be_bytes());
    body.extend(client.as_bytes());
    let mut packet = vec![0x10];
    let mut n = body.len();
    loop {
        let digit = (n % 128) as u8;
        n /= 128;
        packet.push(digit | if n > 0 { 128 } else { 0 });
        if n == 0 {
            break;
        }
    }
    packet.extend(body);
    Ok(packet)
}
async fn packet(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let header = stream.read_u8().await?;
    let mut len = 0usize;
    let mut multiplier = 1;
    for i in 0..4 {
        let digit = stream.read_u8().await?;
        len += (digit as usize & 127) * multiplier;
        if digit & 128 == 0 {
            if len > 65536 {
                return Err(failure("response too large"));
            }
            let mut body = vec![0; len];
            stream.read_exact(&mut body).await?;
            return Ok((header, body));
        }
        if i == 3 {
            return Err(failure("malformed remaining length"));
        }
        multiplier *= 128;
    }
    Err(failure("invalid frame"))
}
async fn connect(address: SocketAddr, id: String) -> io::Result<(TcpStream, u64)> {
    let began = Instant::now();
    let mut stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    stream.write_all(&connect_packet(&id)?).await?;
    let (header, body) = packet(&mut stream).await?;
    if header != 0x20 || body.len() < 3 || body[1] != 0 {
        return Err(failure(&format!(
            "CONNACK header={header:02x} reason={:?}",
            body.get(1)
        )));
    }
    Ok((stream, began.elapsed().as_micros() as u64))
}
fn distribution(values: &mut [u64]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_unstable();
    let mean = values.iter().map(|n| *n as f64).sum::<f64>() / values.len() as f64;
    json!({"count":values.len(),"mean_us":mean,"p99_us":values[(values.len()*99).div_ceil(100)-1],"max_us":values[values.len()-1]})
}
fn count(errors: &mut BTreeMap<String, u64>, message: String) {
    *errors.entry(message).or_default() += 1;
}
struct Attempt {
    lag_us: u64,
    outcome: io::Result<(TcpStream, u64)>,
}
async fn attempt(begin: Instant, due: Duration, address: SocketAddr, id: String) -> Attempt {
    // Sample only when the task actually runs, immediately before its TCP work.
    let lag_us = begin.elapsed().saturating_sub(due).as_micros() as u64;
    let outcome = match tokio::time::timeout(Duration::from_secs(15), connect(address, id)).await {
        Ok(result) => result,
        Err(_) => Err(failure("connect timeout")),
    };
    Attempt { lag_us, outcome }
}
fn completed(
    result: Result<Attempt, tokio::task::JoinError>,
    live: &mut Vec<TcpStream>,
    times: &mut Vec<u64>,
    lag: &mut Vec<u64>,
    errors: &mut BTreeMap<String, u64>,
) {
    match result {
        Ok(attempt) => {
            lag.push(attempt.lag_us);
            match attempt.outcome {
                Ok((stream, time)) => {
                    live.push(stream);
                    times.push(time);
                }
                Err(error) => count(errors, error.to_string()),
            }
        }
        // A panicked task has no trustworthy start timestamp. The case fails rather
        // than inventing a zero-lag sample; raw coverage will also fail validation.
        Err(error) => count(errors, error.to_string()),
    }
}
async fn healthy(live: Vec<TcpStream>) -> (Vec<TcpStream>, BTreeMap<String, u64>) {
    let mut input = live.into_iter();
    let mut tasks = JoinSet::new();
    let mut good = Vec::new();
    let mut errors = BTreeMap::new();
    let mut done = false;
    while !done || !tasks.is_empty() {
        while tasks.len() < 256 && !done {
            if let Some(mut stream) = input.next() {
                tasks.spawn(async move {
                    match tokio::time::timeout(Duration::from_secs(10), async {
                        stream.write_all(&[0xc0, 0]).await?;
                        let (h, b) = packet(&mut stream).await?;
                        if h != 0xd0 || !b.is_empty() {
                            return Err(failure(&format!(
                                "invalid PINGRESP header=0x{h:02x} reason={:?} length={}",
                                b.first(),
                                b.len()
                            )));
                        }
                        Ok::<_, io::Error>(())
                    })
                    .await
                    {
                        Ok(Ok(())) => Ok(stream),
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(failure("ping timeout")),
                    }
                });
            } else {
                done = true
            }
        }
        if let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(stream)) => good.push(stream),
                Ok(Err(e)) => count(&mut errors, e.to_string()),
                Err(e) => count(&mut errors, e.to_string()),
            }
        }
    }
    (good, errors)
}
fn unix_ms() -> io::Result<u64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| failure("wall clock precedes Unix epoch"))?;
    u64::try_from(elapsed.as_millis()).map_err(|_| failure("wall clock milliseconds overflow"))
}
fn observed_wall_ms(shared_start: Option<u64>, begin: Instant) -> io::Result<u64> {
    match shared_start {
        Some(start) => start
            .checked_add(
                u64::try_from(begin.elapsed().as_millis())
                    .map_err(|_| failure("elapsed milliseconds overflow"))?,
            )
            .ok_or_else(|| failure("observed wall time overflow")),
        None => unix_ms(),
    }
}
fn shared_hold_ready(completed: Instant, ping: Instant) -> bool {
    ping.checked_duration_since(completed)
        .is_some_and(|hold| hold >= Duration::from_secs(10))
}
fn raw_step(step: u64, times: &[u64], lag: &[u64]) -> Value {
    json!({"step":step,"latency_us":times,"schedule_lag_us":lag,
        "latency_count":times.len(),"schedule_lag_count":lag.len()})
}
async fn report(file: &mut tokio::fs::File, value: &Value) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    file.seek(io::SeekFrom::Start(0)).await?;
    file.set_len(0).await?;
    file.write_all(&bytes).await?;
    file.flush().await
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = unix_ms()?;
    let options = Options::parse(std::env::args().skip(1), now / 1000, std::process::id())?;
    // Reserve fresh outputs before opening MQTT connections. Alias paths cannot overwrite a report.
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&options.output)
        .await?;
    let mut raw_output = match &options.raw {
        Some(path) => Some(
            tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .await?,
        ),
        None => None,
    };
    let shared = options.schedule(unix_ms()?, Instant::now())?;
    let mut live = Vec::new();
    let mut steps = Vec::new();
    let mut raw_steps = Vec::new();
    // At most2048 concurrent handshakes per process; skipped absolute arrivals are counted.
    for step in 1..=options.plateaus {
        let begin = shared.map_or_else(Instant::now, |schedule| schedule.start);
        tokio::time::sleep_until(begin.into()).await;
        let actual_start = observed_wall_ms(options.start_unix_ms, begin)?;
        let mut tasks = JoinSet::new();
        let mut times = Vec::new();
        let mut errors = BTreeMap::new();
        let mut not_started = 0u64;
        let mut lag = Vec::new();
        for i in 0..options.connections_per_step {
            let due = Duration::from_secs_f64(i as f64 / options.rate as f64);
            tokio::time::sleep_until((begin + due).into()).await;
            while let Some(result) = tasks.try_join_next() {
                completed(result, &mut live, &mut times, &mut lag, &mut errors)
            }
            if tasks.len() >= 2048 {
                not_started += 1;
                continue;
            }
            let index = options.index(step, i)?;
            let address = options.endpoint(index);
            let id = options.client_id(index);
            tasks.spawn(attempt(begin, due, address, id));
        }
        while let Some(result) = tasks.join_next().await {
            completed(result, &mut live, &mut times, &mut lag, &mut errors)
        }
        let ramp_end = Instant::now();
        let completed_wall = observed_wall_ms(options.start_unix_ms, begin)?;
        let elapsed = ramp_end.duration_since(begin).as_secs_f64();
        let accepted = times.len();
        let hold_ok = if let Some(schedule) = shared {
            let okay = shared_hold_ready(ramp_end, schedule.ping);
            tokio::time::sleep_until(schedule.ping.into()).await;
            okay
        } else {
            tokio::time::sleep(Duration::from_secs(10)).await;
            true
        };
        let ping_wall = observed_wall_ms(options.start_unix_ms, begin)?;
        let observed_hold_ms = ramp_end.elapsed().as_millis();
        let (good, ping_errors) = healthy(live).await;
        live = good;
        if raw_output.is_some() {
            raw_steps.push(raw_step(step, &times, &lag));
        }
        let target = step * options.connections_per_step;
        let plateau_ok = live.len() as u64 == target
            && errors.is_empty()
            && ping_errors.is_empty()
            && not_started == 0
            && hold_ok;
        let row = json!({"target":target,"intended_rate":options.rate,
            "new_scheduled":options.connections_per_step,"new_attempted":options.connections_per_step-not_started,
            "not_started":not_started,"new_connack_success":accepted,"new_connack_per_second":accepted as f64/elapsed,
            "ramp_and_drain_seconds":elapsed,"live_after_hold_ping":live.len(),"hold_seconds":10,
            "actual_ramp_start_unix_ms":actual_start,"ramp_completed_unix_ms":completed_wall,
            "ping_started_unix_ms":ping_wall,"observed_hold_ms":observed_hold_ms,
            "shared_hold_at_least_10s":hold_ok,"errors":errors,"ping_errors":ping_errors,
            "connect_latency":distribution(&mut times),"schedule_lag":distribution(&mut lag)});
        println!("{row}");
        steps.push(row);
        let mut result = options.settings();
        result["tool"] = json!("Rust Tokio raw MQTT5 connection ramp");
        result["plateau_passed"] = json!(plateau_ok);
        result["remaining_targets_not_attempted"] = json!(if plateau_ok {
            Vec::<u64>::new()
        } else {
            ((step + 1)..=options.plateaus)
                .map(|n| n * options.connections_per_step)
                .collect()
        });
        result["steps"] = json!(steps);
        report(&mut output, &result).await?;
        if !plateau_ok {
            break;
        }
    }
    for mut stream in live {
        let _ = stream.write_all(&[0xe0, 0]).await;
    }
    if let Some(file) = &mut raw_output {
        report(
            file,
            &json!({"settings":options.settings(),"steps":raw_steps}),
        )
        .await?;
    }
    Ok(())
}
#[cfg(test)]
#[path = "connections/tests.rs"]
mod tests;
