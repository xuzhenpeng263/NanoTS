import nanots


def main() -> None:
    db = nanots.Db("data/sql_demo.ntt")
    db.create_table("sensor", ["value"])
    for i in range(10):
        db.append_row("sensor", 1704067200000 + i * 1000, [float(i)])
    db.flush()

    capsule = db.query_sql_arrow_stream_capsule(
        "SELECT avg(value) AS avg_value FROM sensor WHERE ts_ms >= 1704067200000"
    )
    try:
        import pyarrow as pa
    except Exception as exc:  # pragma: no cover - optional dependency
        raise SystemExit("pyarrow is required for this example") from exc

    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    batch = reader.read_next_batch()
    print(batch)


if __name__ == "__main__":
    main()
