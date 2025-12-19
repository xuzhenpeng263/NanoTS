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

If your PyArrow supports it, you can also do:

```python
import pyarrow.lib
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", t1, t2)
batch = pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)
```
