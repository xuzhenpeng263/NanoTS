import os
import time

try:
    import nanots
except Exception as exc:
    raise SystemExit("nanots python module is required: build with maturin develop") from exc


def main() -> None:
    path = "data/bench_basic.ntt"
    if os.path.exists(path):
        os.remove(path)

    db = nanots.Db(path)
    db.create_table("sensor", ["value"])

    rows = 200_000
    t0 = 1704067200000
    t_start = time.time()
    for i in range(rows):
        db.append_row("sensor", t0 + i * 1000, [float(i)])
    t_write = time.time() - t_start

    t_flush_start = time.time()
    db.flush()
    t_flush = time.time() - t_flush_start

    t_read_start = time.time()
    ts, cols = db.query_table_range_columns("sensor", t0, t0 + rows * 1000)
    t_read = time.time() - t_read_start

    file_size = os.path.getsize(path)

    print("rows:", rows)
    print("write_time_s:", round(t_write, 6))
    print("flush_time_s:", round(t_flush, 6))
    print("read_time_s:", round(t_read, 6))
    print("rows_per_s_write:", int(rows / max(t_write, 1e-9)))
    print("rows_per_s_read:", int(rows / max(t_read, 1e-9)))
    print("file_size_bytes:", file_size)
    print("read_rows:", len(ts), len(cols[0]) if cols else 0)


if __name__ == "__main__":
    main()
