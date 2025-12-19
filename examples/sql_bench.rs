// SPDX-License-Identifier: AGPL-3.0-or-later

use nanots::{NanoTsDb, NanoTsOptions};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(feature = "datafusion")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let threads = args.get(1).and_then(|v| v.parse::<usize>().ok()).unwrap_or(4);
    let iters = args.get(2).and_then(|v| v.parse::<usize>().ok()).unwrap_or(50);

    let mut setup_db = NanoTsDb::open("data/sql_bench.ntt", NanoTsOptions::default())?;
    if setup_db.list_tables()?.is_empty() {
        setup_db.create_table("sensor", &["value"])?;
        let start = 1704067200000i64;
        for i in 0..100_000i64 {
            setup_db.append_row("sensor", start + i, &[i as f64])?;
        }
        setup_db.flush()?;
    }
    drop(setup_db);
    let db = Arc::new(NanoTsDb::open("data/sql_bench.ntt", NanoTsOptions::default())?);

    let sql = "SELECT avg(value) AS avg_value FROM sensor WHERE ts_ms >= 1704067200000";
    let mut handles = Vec::with_capacity(threads);
    let t0 = Instant::now();
    for _ in 0..threads {
        let db = db.clone();
        let sql = sql.to_string();
        handles.push(thread::spawn(move || {
            let mut total = Duration::from_secs(0);
            for _ in 0..iters {
                let start = Instant::now();
                let _ = db.query_sql(&sql).unwrap();
                total += start.elapsed();
            }
            total
        }));
    }

    let mut total = Duration::from_secs(0);
    for h in handles {
        total += h.join().unwrap();
    }
    let elapsed = t0.elapsed();
    let total_queries = threads * iters;
    let avg_ms = total.as_secs_f64() * 1000.0 / total_queries as f64;
    let qps = total_queries as f64 / elapsed.as_secs_f64();

    println!(
        "threads={} iters={} total_queries={} wall={:.2}s avg={:.2}ms qps={:.2}",
        threads,
        iters,
        total_queries,
        elapsed.as_secs_f64(),
        avg_ms,
        qps
    );
    Ok(())
}

#[cfg(not(feature = "datafusion"))]
fn main() {
    eprintln!("This example requires the `datafusion` feature.");
}
