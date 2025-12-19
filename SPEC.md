# NanoTS File Format (v0)

This document describes the on-disk formats currently implemented in the NanoTS Community Edition kernel.

All integers are **little-endian**.

## Database file (single file)

The database is a single file (typically `*.ntt`) with this header:

- `magic`: 4 bytes = `NTSF`
- `version`: u8 = `1`

After the header, the file is a sequence of **records**:

Record header:

- `type`: u8
- `len`: u32 (payload length)
- `payload`: `len` bytes
- `checksum`: u32 (FNV-1a over `type || len || payload`)

Record types:

- `1` = Meta
- `2` = Table schema
- `3` = Table segment
- `4` = WAL record
- `5` = WAL checkpoint
- `6` = Series segment (legacy single-column)
- `7` = Table index (persisted)

### Meta record (type = 1)

Payload:

- `magic`: 4 bytes = `NTSM`
- `version`: u8 = `1`
- `last_seq`: u64
- `retention_ms`: i64 (`-1` means unset)

### Table schema record (type = 2)

Payload:

- `name_len`: u16
- `name_bytes`: `name_len` bytes (UTF-8 table name)
- `schema_bytes`: see **Schema payload** below

### Table segment record (type = 3)

Payload:

- `name_len`: u16
- `name_bytes`: `name_len` bytes (UTF-8 table name)
- `segment_bytes`: see **Table segment payload** below

### WAL record (type = 4)

Payload:

- WAL record bytes starting with `NTWL` (same logical layout as the in-memory WAL)

### WAL checkpoint (type = 5)

Payload:

- empty

### Series segment record (type = 6)

Payload:

- `name_len`: u16
- `name_bytes`: `name_len` bytes (UTF-8 series name)
- `segment_bytes`: see **Series segment payload** below

### Table index record (type = 7)

Payload:

- `name_len`: u16
- `name_bytes`: `name_len` bytes (UTF-8 table name)
- `index_bytes`: see **Table index payload** below

## Schema payload

Header:

- `magic`: 4 bytes = `NTSC`
- `version`: u8 = `1`
- `ncols`: u16

Then repeated `ncols` times:

- `name_len`: u16
- `name_bytes`: `name_len` bytes (UTF-8)

Notes:

- v0 schemas are **Float64-only** value columns; `ts_ms` is implicit.

## Table segment payload (multi-column)

Segment header:

- `magic`: 4 bytes = `NTTB`
- `version`: u8 = `1`
- `min_seq`: u64
- `max_seq`: u64
- `count`: u32 (rows in this segment)
- `min_ts`: i64
- `max_ts`: i64

Extension header (v1+):

- `ext_len`: u16 (bytes)
- `ext_bytes`: `ext_len` bytes (currently empty / reserved for future)

Timestamp block:

- `ts_codec`: u8
  - `1` = `TS_CODEC_TS64` (NanoTS timestamp compressor)
- `ts_len`: u32 (bytes)
- `ts_bytes`: `ts_len` bytes (compressed `i64` milliseconds)

Column blocks:

- `ncols`: u16 (must match schema)

Then repeated `ncols` times:

- `col_codec`: u8
  - `2` = `TABLE_COL_F64_XOR` (Gorilla XOR-style)
  - `3` = `TABLE_COL_I64_D2` (auto fallback when Float64 values are exact integers; stored as compressed `i64`)
- `col_len`: u32 (bytes)
- `col_bytes`: `col_len` bytes (per codec)

## Series segment payload (legacy single-column)

Segment header:

- `magic`: 4 bytes = `NTSG`
- `version`: u8 = `1`
- `min_seq`: u64
- `max_seq`: u64
- `count`: u32
- `min_ts`: i64
- `max_ts`: i64

Timestamp block:

- `ts_codec`: u8
  - `1` = `TS_CODEC_TS64`
- `ts_len`: u32
- `ts_bytes`: `ts_len` bytes

Value block:

- `val_len`: u32
- `val_bytes`: `val_len` bytes (`count * 8` little-endian `f64`)

Notes:

- Series layout is kept for backward compatibility and benchmarks; the kernel API uses **tables** (including 1-column tables named `value`).

## Table index payload

Header:

- `magic`: 4 bytes = `NTSI`
- `version`: u8 = `1`
- `entry_count`: u32

Then repeated `entry_count` times:

- `offset`: u64 (segment offset inside `.ntt`)
- `len`: u64 (segment length bytes)
- `min_ts`: i64
- `max_ts`: i64
- `min_seq`: u64
- `max_seq`: u64
- `count`: u32 (rows in segment)
