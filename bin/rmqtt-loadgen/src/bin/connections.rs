//! Bounded, absolute-rate MQTT5 connection ramp; independent of message timing.
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
};

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
fn completed(
    result: Result<Result<(TcpStream, u64), io::Error>, tokio::task::JoinError>,
    live: &mut Vec<TcpStream>,
    times: &mut Vec<u64>,
    errors: &mut BTreeMap<String, u64>,
) {
    match result {
        Ok(Ok((stream, time))) => {
            live.push(stream);
            times.push(time)
        }
        Ok(Err(e)) => count(errors, e.to_string()),
        Err(e) => count(errors, e.to_string()),
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
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let addrs: Vec<SocketAddr> = args
        .next()
        .ok_or_else(|| failure("need endpoints rate output"))?
        .split(',')
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    let rate: u64 = args.next().ok_or_else(|| failure("need rate"))?.parse()?;
    let output = PathBuf::from(args.next().ok_or_else(|| failure("need output"))?);
    if addrs.len() != 3 || rate == 0 || rate > 10000 || output.exists() {
        return Err(failure("need3 endpoints, rate1..10000, fresh output").into());
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let mut live = Vec::new();
    let mut steps = Vec::new();
    // At most2048 concurrent handshakes; skipped absolute arrivals are counted.
    for step in 1..=10u64 {
        let begin = Instant::now();
        let mut tasks = JoinSet::new();
        let mut times = Vec::new();
        let mut errors = BTreeMap::new();
        let mut not_started = 0u64;
        let mut lag = Vec::new();
        for i in 0..10000u64 {
            tokio::time::sleep_until(
                (begin + Duration::from_secs_f64(i as f64 / rate as f64)).into(),
            )
            .await;
            while let Some(result) = tasks.try_join_next() {
                completed(result, &mut live, &mut times, &mut errors)
            }
            if tasks.len() >= 2048 {
                not_started += 1;
                continue;
            }
            lag.push(
                begin
                    .elapsed()
                    .saturating_sub(Duration::from_secs_f64(i as f64 / rate as f64))
                    .as_micros() as u64,
            );
            let index = (step - 1) * 10000 + i;
            let address = addrs[index as usize % addrs.len()];
            tasks.spawn(async move {
                tokio::time::timeout(
                    Duration::from_secs(15),
                    connect(address, format!("c{stamp:010}{index:06}")),
                )
                .await
                .map_err(|_| failure("connect timeout"))?
            });
        }
        while let Some(result) = tasks.join_next().await {
            completed(result, &mut live, &mut times, &mut errors)
        }
        let elapsed = begin.elapsed().as_secs_f64();
        let accepted = times.len();
        tokio::time::sleep(Duration::from_secs(10)).await;
        let (good, ping_errors) = healthy(live).await;
        live = good;
        let row = json!({"target":step*10000,"intended_rate":rate,"new_scheduled":10000,"new_attempted":10000-not_started,"not_started":not_started,"new_connack_success":accepted,"new_connack_per_second":accepted as f64/elapsed,"ramp_and_drain_seconds":elapsed,"live_after_hold_ping":live.len(),"hold_seconds":10,"errors":errors,"ping_errors":ping_errors,"connect_latency":distribution(&mut times),"schedule_lag":distribution(&mut lag)});
        println!("{row}");
        let plateau_ok = live.len() as u64 == step * 10000
            && errors.is_empty()
            && ping_errors.is_empty()
            && not_started == 0;
        steps.push(row);
        let report = json!({"tool":"Rust Tokio raw MQTT5 connection ramp","endpoints":addrs.iter().map(ToString::to_string).collect::<Vec<_>>(),"rate":rate,"max_concurrent_handshakes":2048,"timeout_seconds":15,"plateau_passed":plateau_ok,"remaining_targets_not_attempted":if plateau_ok { Vec::<u64>::new() } else { ((step+1)..=10).map(|n|n*10000).collect() },"steps":steps});
        tokio::fs::write(&output, serde_json::to_vec_pretty(&report)?).await?;
        // Do not label higher targets measured after a lower plateau failed.
        if !plateau_ok {
            break;
        }
    }
    for mut stream in live {
        let _ = stream.write_all(&[0xe0, 0]).await;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mqtt5_connect_fields() {
        let p = connect_packet("abc").unwrap();
        assert_eq!(&p[..2], &[0x10, 16]);
        assert_eq!(&p[2..13], &[0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 0, 0]);
        assert_eq!(&p[13..], &[0, 3, b'a', b'b', b'c']);
    }
    #[tokio::test]
    async fn successful_connack_and_ping_are_wire_validated() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (header, _) = packet(&mut socket).await.unwrap();
            assert_eq!(header, 0x10);
            socket.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
            assert_eq!(packet(&mut socket).await.unwrap().0, 0xc0);
            socket.write_all(&[0xd0, 0]).await.unwrap();
        });
        let (stream, _) = connect(address, "wire-test".into()).await.unwrap();
        let (live, errors) = healthy(vec![stream]).await;
        assert_eq!(live.len(), 1);
        assert!(errors.is_empty());
        peer.await.unwrap();
    }
    #[test]
    fn exact_nearest_rank() {
        let mut n = vec![2, 1, 3, 4];
        let d = distribution(&mut n);
        assert_eq!(d["p99_us"], 4);
        assert_eq!(d["mean_us"], 2.5);
    }
}
