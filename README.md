# NanoTS (Community Edition)

An **embeddable**, **append-only**, **high-performance** time-series storage kernel written in **Rust**.

- **License**: AGPL-3.0-or-later
- **Positioning**: SQLite-like *in-process* TSDB kernel (single-node, edge/embedded)
- **Docs**: English ([README.md](README.md), [CONTRIBUTING.md](CONTRIBUTING.md), [SPEC.md](SPEC.md)), 中文 ([README.zh-CN.md](README.zh-CN.md), [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md), [SPEC.zh-CN.md](SPEC.zh-CN.md))

## Status & compatibility

This project is pre-release. The on-disk format may change at any time.

## Benchmarks (local, synthetic data)

These numbers are from local runs and synthetic data (perfect/smooth patterns). Treat them as directional only.

| Test | Dataset | Command | Result |
| --- | --- | --- | --- |
| Compression size | 100k points, single-column `f64`, single table | `python test_final.py` | `.ntt` size 5612.29 KB (pre-pack), 624.74 KB after pack; raw ~800 KB |
| Compression ratio | 100k points, single table | `python test_compression_ratio.py` | raw_total 1562.50 KB, on_disk 19.57 KB, ratio 79.82x |
| Read perf | 100k points, single table | `python test_read_perf.py` | avg 15.58 ms, p50 14.48 ms, p90 18.91 ms, p99 21.19 ms, 6,420,029 rows/s |


## What you get

- **In-process** library API (no built-in HTTP server)
- **Single-file DB** (shareable `.ntt` file)
- **Append-only storage + WAL + crash recovery**
- **Columnar tables** (v0: implicit `ts_ms`, value columns are `f64`)
- **Retention/TTL** via manual compaction
- **Adaptive value compression**
  - `col_codec=2`: Gorilla XOR for floats
  - `col_codec=3`: auto fallback when a float column is exact integers → encode as `i64` using NanoTS integer compressor
- **mmap read path** for disk reads (fewer syscalls, OS page cache friendly)
- **Apache Arrow C Data Interface** export (zero-copy interop)
- **Python bindings** (PyO3/maturin) with Arrow capsule import
- **Configurable record size limit** via `NANOTS_MAX_RECORD_SIZE` (default: 64MB)

## Non-goals (Community Edition)

- Clustering / Raft
- Custom SQL parser (use an external engine like DataFusion if needed)
- Built-in dashboard / web UI (use Grafana)
- Auth/RBAC in the kernel

## Repository layout

- Rust kernel: `src/` (crate name: `nanots`)
- C header: `include/nanots.h`
- Python binding crate: `python/nanots-py/`
- File format spec: [SPEC.md](SPEC.md)

## Data model

- A **table** has an implicit timestamp column `ts_ms: i64` (milliseconds).
- Each table has **N value columns**, currently `f64` only (Float64 v0).
- Data is appended as rows: `append_row(table, ts_ms, values[])`.

Notes:

- `append(series, ts_ms, value)` is a convenience API that writes to a single-column table.
- Schema is stored in the `.ntt` file (see [SPEC.md](SPEC.md)).

## SQL (limited)

- Supported statements: `CREATE TABLE`, `INSERT INTO ... VALUES ...`
- Identifiers may be unquoted or quoted using double quotes or backticks (e.g. `"my_table"`, `` `my_table` ``).

## Rust usage (detailed)

### Add as a dependency (local path)

This repo is not published to crates.io. In your project:

```toml
[dependencies]
nanots = { path = "/path/to/ts_compressor_rust", features = ["arrow"] }
```

### Basic write & flush

```rust
use nanots::{NanoTsDb, NanoTsOptions};

let mut db = NanoTsDb::open("data/nanots.ntt", NanoTsOptions::default())?;
db.create_table("sensor", &["temp", "humidity"])?;
db.append_row("sensor", 1704067200000, &[25.5, 60.0])?;
db.flush()?;
# Ok::<(), std::io::Error>(())
```

### Multi-column, range query, retention, manual pack

```rust
use nanots::{NanoTsDb, NanoTsOptions};
use std::time::Duration;

let opts = NanoTsOptions {
    retention: Some(Duration::from_secs(3600)), // 1 hour
    ..Default::default()
};
let mut db = NanoTsDb::open("data/nanots.ntt", opts)?;

db.create_table("sensor", &["temp", "humidity"])?;

let t0 = 1704067200000i64;
for i in 0..10_000i64 {
    let ts = t0 + i * 1000;
    db.append_row("sensor", ts, &[25.0 + (i as f64) * 0.001, 60.0])?;
}

db.flush()?;

let (ts_ms, cols) = db.query_table_range_columns("sensor", t0, t0 + 60_000)?;
assert_eq!(cols.len(), 2);
println!("rows={}", ts_ms.len());

db.compact_retention_now()?;
db.pack_table("sensor", 8192)?;

# Ok::<(), std::io::Error>(())
```

### Auto maintenance (background pack + retention)

```rust
use nanots::{AutoMaintenanceOptions, NanoTsDbShared, NanoTsOptions};
use std::time::Duration;

let opts = NanoTsOptions {
    retention: Some(Duration::from_secs(3600)),
    auto_maintenance: Some(AutoMaintenanceOptions::default()),
    ..Default::default()
};
let db = NanoTsDbShared::open("data/nanots.ntt", opts)?;
```

### Rust API map (quick reference)

- `NanoTsDb::open(path, opts)`: open/create a DB directory
- `create_table(table, columns)`: create a table schema
- `append_row(table, ts_ms, values)`: append one row (WAL first)
- `append(series, ts_ms, value)`: convenience single-column append
- `flush()`: flush buffers to `.ntt`, checkpoint & truncate WAL
- `query_table_range_columns(table, start, end)`: returns `(ts_ms, cols)`
- `pack_table(table, target_segment_points)`: manual compaction (rewrite segments)
- `compact_retention_now()`: drop points older than retention

## Python usage (detailed)

### Build & install (developer mode)

Requirements: Rust toolchain + Python + `maturin`.

```bash
cd python/nanots-py
maturin develop --release
```

This installs a local `nanots` extension module into the active Python environment.
Type hints are included via `python/nanots-py/nanots.pyi`.

### Basic write & flush

```python
import nanots

db = nanots.Db("./my_db.ntt", 3600 * 1000)  # retention: 1 hour (ms)
db.append("sensor_x", 1704067200000, 25.5)
db.flush()
```

### Multi-column tables

```python
import nanots

db = nanots.Db("./my_db.ntt")
db.create_table("sensor", ["temp", "humidity"])

t0 = 1704067200000
for i in range(10000):
    ts = t0 + i * 1000
    db.append_row("sensor", ts, [25.0 + i * 0.001, 60.0])

db.flush()
ts_ms, cols = db.query_table_range_columns("sensor", t0, t0 + 60000)
print("rows:", len(ts_ms), "cols:", len(cols))
```

### Arrow zero-copy to PyArrow / Pandas

```python
import nanots
import pyarrow.lib

db = nanots.Db("./my_db.ntt")
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", 0, 10**18)
batch = pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)

df = batch.to_pandas()  # zero-copy where possible on the Arrow side
print(df.head())
```

### Arrow batch append (from PyArrow)

```python
import nanots
import pyarrow as pa

db = nanots.Db("./my_db.ntt")
batch = pa.record_batch(
    [
        pa.array([1000, 1001, 1002], type=pa.int64()),
        pa.array([1.0, 2.0, 3.0], type=pa.float64()),
    ],
    names=["ts_ms", "value"],
)
schema_capsule, array_capsule = batch.__arrow_c_array__()
db.append_rows_arrow_capsules("sensor", schema_capsule, array_capsule)
```

### Stats (space accounting & codec verification)

```python
import nanots

db = nanots.Db("./my_db.ntt")
print(db.stats("sensor"))
```

### Manual pack / compaction

```python
import nanots

db = nanots.Db("./my_db.ntt")
db.pack_table("sensor", 8192)
```

## File format

See [SPEC.md](SPEC.md) for the single-file record layout and payload formats.

## Build & test

Kernel tests (offline friendly):

```bash
cargo test --offline --features arrow
```
