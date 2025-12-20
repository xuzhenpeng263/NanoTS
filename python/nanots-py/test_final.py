import os

import nanots_db

try:
    import numpy as np
except Exception as e:
    raise SystemExit("numpy is required for this test: pip install numpy") from e

try:
    import pyarrow as pa
except Exception as e:
    raise SystemExit("pyarrow is required for this test: pip install pyarrow") from e


DB_PATH = "./bench_db.ntt"

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

print(">>> init db")
db = nanots_db.Db(DB_PATH, 3600 * 1000)

print(">>> write 100k points")
ts_base = 1704067200000
count = 100000
for i in range(count):
    val = 25.0 + float(np.sin(i * 0.01))
    db.append("sensor_x", ts_base + i * 1000, val)

print(">>> flush")
db.flush()

size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
print(f">>> .ntt size: {size / 1024:.2f} KB (raw would be ~800 KB for 100k f64)")

print(">>> pack (compact)")
db.pack_table("sensor_x", 8192)

size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
print(f">>> .ntt size after pack: {size / 1024:.2f} KB")

print(">>> arrow read (zero-copy)")
schema_capsule, array_capsule = db.query_table_range_arrow_capsules(
    "sensor_x", ts_base, ts_base + count * 1000
)

import pyarrow.lib
batch = pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)

print(f">>> rows: {batch.num_rows}")
assert batch.num_rows == count

print(">>> ok")
