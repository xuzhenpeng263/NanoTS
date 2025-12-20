// SPDX-License-Identifier: AGPL-3.0-or-later

use nanots::{NanoTsDb, NanoTsOptions};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if has_flag(&args, "--writer") {
        return run_writer(&args);
    }
    run_supervisor(&args)
}

fn run_supervisor(args: &[String]) -> io::Result<()> {
    let path = arg_value(args, "--path")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/power_loss.ntt"));
    let expected_path = arg_value(args, "--expected")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("{}.expected", path.display())));
    let rounds = arg_u64(args, "--rounds", 20);
    let max_writes = arg_u64(args, "--max-writes", 100_000);
    let kill_min_ms = arg_u64(args, "--kill-min-ms", 50);
    let kill_max_ms = arg_u64(args, "--kill-max-ms", 200);
    let sync_every = arg_u64(args, "--expected-sync-every", 1);

    if has_flag(args, "--reset") {
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&expected_path);
    }

    let mut seed = seed_from_time();
    for round in 0..rounds {
        let exe = env::current_exe()?;
        let mut child = Command::new(exe)
            .arg("--writer")
            .arg("--path")
            .arg(&path)
            .arg("--expected")
            .arg(&expected_path)
            .arg("--max-writes")
            .arg(max_writes.to_string())
            .arg("--expected-sync-every")
            .arg(sync_every.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let delay = rand_range_ms(&mut seed, kill_min_ms, kill_max_ms);
        thread::sleep(Duration::from_millis(delay));
        let _ = child.kill();
        let _ = child.wait();

        verify_db(&path, &expected_path)?;
        let expected_count = read_expected(&expected_path)?.len();
        println!("round {} ok: expected_rows={}", round + 1, expected_count);
    }

    Ok(())
}

fn run_writer(args: &[String]) -> io::Result<()> {
    let path = arg_value(args, "--path")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/power_loss.ntt"));
    let expected_path = arg_value(args, "--expected")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("{}.expected", path.display())));
    let max_writes = arg_u64(args, "--max-writes", 100_000);
    let sync_every = arg_u64(args, "--expected-sync-every", 1).max(1) as usize;

    let mut db = NanoTsDb::open(&path, NanoTsOptions::default())?;
    let _ = db.create_table("t", &["v"]);

    let start_seq = read_last_seq(&expected_path)?.unwrap_or(0).saturating_add(1);
    let mut expected = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&expected_path)?;

    for i in 0..max_writes {
        let seq = start_seq.saturating_add(i);
        db.append_row("t", seq as i64, &[seq as f64])?;
        writeln!(expected, "{}", seq)?;
        if (i as usize + 1) % sync_every == 0 {
            expected.flush()?;
            expected.sync_data()?;
        }
    }
    Ok(())
}

fn verify_db(path: &Path, expected_path: &Path) -> io::Result<()> {
    let expected = read_expected(expected_path)?;
    if expected.is_empty() {
        return Ok(());
    }
    let max_ts = *expected.last().unwrap() as i64;

    let db = NanoTsDb::open(path, NanoTsOptions::default())?;
    let (ts, cols) = db.query_table_range_columns("t", 0, max_ts)?;
    if cols.len() != 1 || ts.len() != cols[0].len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "column length mismatch",
        ));
    }

    let mut seen: HashMap<i64, f64> = HashMap::with_capacity(ts.len());
    for (t, v) in ts.iter().copied().zip(cols[0].iter().copied()) {
        if v != t as f64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "value mismatch",
            ));
        }
        seen.insert(t, v);
    }

    for seq in expected {
        let ts = seq as i64;
        if !seen.contains_key(&ts) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing expected row",
            ));
        }
    }
    Ok(())
}

fn read_expected(path: &Path) -> io::Result<Vec<u64>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(val) = trimmed.parse::<u64>() {
            out.push(val);
        }
    }
    Ok(out)
}

fn read_last_seq(path: &Path) -> io::Result<Option<u64>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    if buf.is_empty() {
        return Ok(None);
    }
    let mut pos = buf.len();
    while pos > 0 && buf[pos - 1] == b'\n' {
        pos -= 1;
    }
    while pos > 0 && buf[pos - 1] != b'\n' {
        pos -= 1;
    }
    let line = &buf[pos..];
    let s = std::str::from_utf8(line).unwrap_or("").trim();
    if s.is_empty() {
        return Ok(None);
    }
    match s.parse::<u64>() {
        Ok(v) => Ok(Some(v)),
        Err(_) => Ok(None),
    }
}

fn arg_value(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == key)
        .and_then(|idx| args.get(idx + 1))
        .cloned()
}

fn arg_u64(args: &[String], key: &str, default: u64) -> u64 {
    arg_value(args, key)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn has_flag(args: &[String], key: &str) -> bool {
    args.iter().any(|arg| arg == key)
}

fn seed_from_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn rand_range_ms(seed: &mut u64, min: u64, max: u64) -> u64 {
    let span = max.saturating_sub(min).max(1);
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    min + (*seed % span)
}
