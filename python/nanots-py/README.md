# nanots (Python)

Python bindings for NanoTS (Community Edition).

## Build (local)

```bash
cd python/nanots-py
maturin develop --release
python -c "import nanots; print(nanots.__version__)"
```

## Arrow zero-copy export

`Db.query_table_range_arrow_capsules(...)` returns `(array_capsule, schema_capsule)` compatible with:

```python
import pyarrow as pa
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", t1, t2)
struct_arr = pa.Array._import_from_c(array_capsule, schema_capsule)
# struct_arr is a StructArray with fields: ts_ms + schema columns
table = pa.Table.from_struct_array(struct_arr)
```

For best throughput, prefer Arrow export over Python object conversion.

If your PyArrow supports it, you can also do:

```python
import pyarrow.lib
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", t1, t2)
batch = pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)
```

## Arrow batch append

```python
import pyarrow as pa
import nanots

db = nanots.Db("data/test.ntt")

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

Notes:

- This path uses a batch append internally (columnar), avoiding per-row Python/Rust overhead.
- Column names must match the table schema; include all columns in order.
- For Float64-only tables, `ts_ms` + all value columns must be present.

## Auto maintenance options

```python
db = nanots.Db(
    "data/test.ntt",
    auto_maintenance={
        "wal_target_ratio": 2,
        "wal_min_bytes": 32 * 1024 * 1024,
        "target_segment_points": 8192,
        "check_interval_ms": 5000,
        "retention_check_interval_ms": 60000,
        "max_writes_per_sec": 100000,
        "write_load_window_ms": 5000,
        "min_idle_ms": 1000,
        "max_segments_per_pack": 64,
    },
)
```

## Diagnose

```python
import nanots

db = nanots.Db("data/test.ntt")
print(db.diagnose())
```

## Power loss simulation

```bash
python test_power_loss.py --reset --rounds 20
```
