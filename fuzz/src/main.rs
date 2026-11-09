use lyrnormq_fuzz_harness::{MAX_INPUT, codec, seeds, session, session_v5};
use std::{error::Error, path::PathBuf};
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("seed-corpus") {
        let dir = PathBuf::from(args.get(1).ok_or("seed-corpus requires directory")?);
        std::fs::create_dir_all(&dir)?;
        for (i, data) in seeds().iter().enumerate() {
            std::fs::write(dir.join(format!("seed-{i:02}")), data)?;
        }
        return Ok(());
    }
    let runs: usize = args
        .first()
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(20000);
    let seed: u64 = args
        .get(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(0x4d51545435);
    if runs > 1_000_000 {
        return Err("run budget exceeds 1000000".into());
    }
    let mut state = seed;
    let corpus = seeds();
    let mut accepted_bytes = 0usize;
    for i in 0..runs {
        let mut data = corpus[i % corpus.len()].clone();
        for _ in 0..(i % 8 + 1) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let n = state as usize;
            match n % 5 {
                0 if !data.is_empty() => {
                    let at = n % data.len();
                    data[at] ^= (n >> 8) as u8;
                }
                1 => data.truncate(n % (data.len() + 1)),
                2 if data.len() < MAX_INPUT => data.insert(n % (data.len() + 1), (n >> 16) as u8),
                3 if data.len() < MAX_INPUT => data.push((n >> 24) as u8),
                _ => {}
            }
        }
        accepted_bytes += data.len();
        codec(&data);
        session(&data);
        session_v5(&data);
    }
    println!(
        "PASS stable seeded mutation campaign runs={runs} seed={seed} total_input_bytes={accepted_bytes} max_input={MAX_INPUT}"
    );
    Ok(())
}
