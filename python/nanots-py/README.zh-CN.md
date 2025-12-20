# nanots（Python）

NanoTS（社区版）的 Python 绑定。

## 构建（本地）

```bash
cd python/nanots-py
maturin develop --release
python -c "import nanots; print(nanots.__version__)"
```

## Arrow 零拷贝导出

`Db.query_table_range_arrow_capsules(...)` 返回 `(array_capsule, schema_capsule)`，可用于：

```python
import pyarrow as pa
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", t1, t2)
struct_arr = pa.Array._import_from_c(array_capsule, schema_capsule)
# struct_arr 是一个 StructArray，字段：ts_ms + schema 列
table = pa.Table.from_struct_array(struct_arr)
```

为获得最佳吞吐量，优先使用 Arrow 导出而非 Python 对象转换。

如果你的 PyArrow 支持，也可以这样：

```python
import pyarrow.lib
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", t1, t2)
batch = pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)
```

## 自动维护选项

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
    },
)
```

## 断电模拟

```bash
python test_power_loss.py --reset --rounds 20
```
