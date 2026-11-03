#![allow(dead_code)]
#[path = "../../../../../crates/mqtt-cluster/src/model.rs"]
mod model;
#[path = "../../../../../crates/mqtt-cluster/src/repair_state.rs"]
mod repair_state;
use serde_json::{Value, json};
use std::{hint::black_box, io, time::Instant};
type Error = Box<dyn std::error::Error>;
fn sample(mut f: impl FnMut(usize) -> Result<usize, Error>) -> Result<Value, Error> {
    let mut us = Vec::new();
    for i in 0..30 {
        let begin = Instant::now();
        black_box(f(i)?);
        us.push(begin.elapsed().as_secs_f64() * 1e6)
    }
    us.sort_by(f64::total_cmp);
    Ok(
        json!({"n":us.len(),"mean_us":us.iter().sum::<f64>()/us.len() as f64,"p99_us":us[us.len()-1]}),
    )
}
fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let input = args
        .next()
        .ok_or_else(|| io::Error::other("need input and fresh db prefix"))?;
    let prefix = args
        .next()
        .ok_or_else(|| io::Error::other("need db prefix"))?;
    let original: model::State = serde_json::from_slice(&std::fs::read(input)?)?;
    let mut results = Vec::new();
    for n in [0usize, 100, 1000, original.metadata.durable.len()] {
        let mut s = original.clone();
        s.metadata.durable = s.metadata.durable.into_iter().take(n).collect();
        s.metadata.clients = s.metadata.clients.into_iter().take(n).collect();
        let command = || model::Command::Expire {
            node: u64::MAX,
            incarnation: "offline-profile".into(),
            expires_at: 0,
        };
        if let Some(e) = s.metadata.apply(command()).error {
            return Err(io::Error::other(e).into());
        }
        let metadata = serde_json::to_vec(&s.metadata)?;
        let state = serde_json::to_vec(&s)?;
        let clone = sample(|_| {
            let c = black_box(s.clone());
            Ok(c.metadata.durable.len())
        })?;
        let encode = sample(|_| Ok(black_box(serde_json::to_vec(&s)?).len()))?;
        let barrier = sample(|_| {
            let m = s.metadata.clone();
            let b = serde_json::to_vec(&m)?;
            let d: model::Metadata = serde_json::from_slice(&b)?;
            Ok(black_box(d).durable.len())
        })?;
        let apply = sample(|_| {
            let mut next = s.clone();
            if let Some(e) = next.metadata.apply(command()).error {
                return Err(io::Error::other(e).into());
            }
            Ok(black_box(serde_json::to_vec(&next)?).len())
        })?;
        let path = format!("{prefix}.{n}.sqlite");
        if std::path::Path::new(&path).exists() {
            return Err(io::Error::other("diagnostic DB exists; use fresh prefix").into());
        }
        let db = rusqlite::Connection::open(path)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE meta(key TEXT PRIMARY KEY,data BLOB NOT NULL)")?;
        let variants: Vec<Vec<u8>> = (0..30)
            .map(|i| {
                let mut c = s.clone();
                c.metadata.revision += i + 1;
                serde_json::to_vec(&c)
            })
            .collect::<Result<_, _>>()?;
        let write = sample(|i| {
            db.execute("INSERT INTO meta(key,data) VALUES('state',?1) ON CONFLICT(key) DO UPDATE SET data=excluded.data",[&variants[i]])?;
            Ok(variants[i].len())
        })?;
        results.push(json!({"durable_owners":n,"routes":s.metadata.routes.len(),"metadata_bytes":metadata.len(),"state_bytes":state.len(),"state_clone":clone,"state_encode":encode,"full_metadata_read_cpu_clone_encode_decode":barrier,"nonheartbeat_state_apply_clone_rebuild_encode_cpu":apply,"sqlite_full_state_preencoded_wal_full_upsert":write}));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"kind":"offline decomposition, not live Raft timing", "iterations":30,"results":results})
        )?
    );
    Ok(())
}
