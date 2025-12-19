// SPDX-License-Identifier: AGPL-3.0-or-later

use nanots::{NanoTsDb, NanoTsOptions};
use std::sync::Arc;

#[cfg(feature = "datafusion")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(NanoTsDb::open("data/sql_demo.ntt", NanoTsOptions::default())?);
    db.create_table("sensor", &["value"])?;
    for i in 0..10i64 {
        db.append_row("sensor", 1704067200000 + i * 1000, &[i as f64])?;
    }
    db.flush()?;

    let sql = "SELECT avg(value) AS avg_value FROM sensor WHERE ts_ms >= 1704067200000";
    let batches = db.query_sql(sql)?;
    let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&batches)?;
    println!("{pretty}");
    Ok(())
}

#[cfg(not(feature = "datafusion"))]
fn main() {
    eprintln!("This example requires the `datafusion` feature.");
}
