import argparse
import random
import statistics
import time

import nanots


DEFAULT_START_MS = -2**62
DEFAULT_END_MS = 2**62


def pct(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(round((p / 100.0) * (len(s) - 1)))
    return s[idx]


def fmt_ms(x):
    return f"{x*1000:.2f} ms"


def import_recordbatch(schema_capsule, array_capsule):
    try:
        import pyarrow.lib

        return pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)
    except Exception as e:
        raise RuntimeError(
            "PyArrow is required for --mode arrow (pip install pyarrow). "
            "Import failed with: "
            + repr(e)
        )




def run_query(db, table, start_ms, end_ms, mode):
    if mode == "columns":
        ts, cols = db.query_table_range_columns(table, start_ms, end_ms)
        return len(ts)
    if mode == "arrow":
        schema_capsule, array_capsule = db.query_table_range_arrow_capsules(table, start_ms, end_ms)
        batch = import_recordbatch(schema_capsule, array_capsule)
        return batch.num_rows
    raise ValueError(f"unknown mode: {mode}")


def main():
    ap = argparse.ArgumentParser(description="NanoTS read performance smoke benchmark")
    ap.add_argument("--db", default="./bench_db_ratio.ntt", help="DB path")
    ap.add_argument("--table", default="sensor_x", help="table/series name")
    ap.add_argument(
        "--mode",
        choices=["columns", "arrow"],
        default="columns",
        help="read path to benchmark",
    )
    ap.add_argument("--start-ms", type=int, default=DEFAULT_START_MS, help="query start (inclusive)")
    ap.add_argument("--end-ms", type=int, default=DEFAULT_END_MS, help="query end (inclusive)")
    ap.add_argument(
        "--window-ms",
        type=int,
        default=0,
        help="if >0, run random window queries of this width",
    )
    ap.add_argument(
        "--auto-range",
        action="store_true",
        help="infer min/max ts from the table index if start/end are not provided",
    )
    ap.add_argument("--iters", type=int, default=20, help="timed iterations")
    ap.add_argument("--warmup", type=int, default=3, help="warmup iterations (not timed)")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed for window mode")
    args = ap.parse_args()

    db = nanots.Db(args.db)

    try:
        st = db.stats(args.table)
        rows_total = int(st.get("rows", 0))
        on_disk = int(st.get("on_disk_bytes", 0))
        print(f"stats: rows={rows_total} segments={st.get('segments')} on_disk={on_disk/1024:.2f} KB")
    except Exception as e:
        print(f"stats: unavailable ({e!r})")

    if args.auto_range and args.start_ms == DEFAULT_START_MS and args.end_ms == DEFAULT_END_MS:
        inferred = db.table_time_range(args.table)
        if inferred is not None:
            args.start_ms, args.end_ms = inferred
            print(f"range: inferred [{args.start_ms}, {args.end_ms}] from single-file index")
        else:
            print("range: could not infer from table index; using defaults")

    rng = random.Random(args.seed)

    def pick_range():
        if args.window_ms and args.window_ms > 0:
            span = args.end_ms - args.start_ms
            if span <= args.window_ms:
                return args.start_ms, args.end_ms
            s = args.start_ms + rng.randrange(0, span - args.window_ms)
            return s, s + args.window_ms
        return args.start_ms, args.end_ms

    for _ in range(args.warmup):
        s, e = pick_range()
        run_query(db, args.table, s, e, args.mode)

    times = []
    rows = []
    for _ in range(args.iters):
        s, e = pick_range()
        t0 = time.perf_counter()
        n = run_query(db, args.table, s, e, args.mode)
        dt = time.perf_counter() - t0
        times.append(dt)
        rows.append(n)

    total_rows = sum(rows)
    total_time = sum(times)
    rps = (total_rows / total_time) if total_time > 0 else 0.0

    print(f"mode={args.mode} iters={args.iters} window_ms={args.window_ms}")
    print(f"rows: total={total_rows} avg/iter={statistics.mean(rows):.0f}")
    print(
        "time:",
        f"avg={fmt_ms(statistics.mean(times))}",
        f"p50={fmt_ms(pct(times,50))}",
        f"p90={fmt_ms(pct(times,90))}",
        f"p99={fmt_ms(pct(times,99))}",
        f"max={fmt_ms(max(times))}",
    )
    print(f"throughput: {rps:,.0f} rows/s")


if __name__ == "__main__":
    main()
