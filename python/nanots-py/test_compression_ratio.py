import os

import nanots_db

try:
    import numpy as np
except Exception as e:
    raise SystemExit("numpy is required for this test: pip install numpy") from e


DB_PATH = "./bench_db_ratio.ntt"
TABLE = "sensor_x"
COUNT = 100_000
TS_BASE = 1704067200000

# Thresholds (tune as needed)
MIN_TOTAL_RATIO_X = 1.10  # (raw ts+values) / (ntt+idx)


if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

print(">>> init db")
db = nanots_db.Db(DB_PATH, 3600 * 1000)

print(">>> write 100k points")
for i in range(COUNT):
    # Integer-like signal (counter/state), stored as f64 but should trigger TABLE_COL_I64_D2 fallback.
    val = float(i % 100)
    db.append(TABLE, TS_BASE + i * 1000, val)

print(">>> flush")
db.flush()

print(">>> stats")
st = db.stats(TABLE)
print(st)

raw_total = int(st["raw_total_bytes"])
on_disk = int(st["on_disk_bytes"])
ratio_total = float(st.get("compression_ratio_total_x", 0.0))
ratio_ts = float(st.get("compression_ratio_ts_blob_x", 0.0))
ratio_val = float(st.get("compression_ratio_value_blob_x", 0.0))

print(f">>> raw_total: {raw_total/1024:.2f} KB")
print(f">>> on_disk  : {on_disk/1024:.2f} KB (.ntt)")
print(f">>> ratio    : {ratio_total:.2f}x")
print(f">>> ts_blob  : {ratio_ts:.2f}x")
print(f">>> val_blob : {ratio_val:.2f}x")

assert int(st["rows"]) == COUNT
assert on_disk > 0
assert ratio_total >= MIN_TOTAL_RATIO_X, f"ratio {ratio_total:.2f}x < {MIN_TOTAL_RATIO_X:.2f}x"

print(">>> ok")
