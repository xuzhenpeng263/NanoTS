# NanoTS 文件格式（v0）

本文档描述 NanoTS 社区版内核当前实现的磁盘格式。

所有整数均为**小端序**。

## 数据库文件（单文件）

数据库为单文件（通常为 `*.ntt`），文件头为：

- `magic`：4 bytes = `NTSF`
- `version`：u8 = `2`

文件头之后是连续的**记录（record）**：

记录头：

- `type`：u8
- `len`：u32（payload 长度）
- `payload`：`len` bytes
- `checksum`：u32（对 `type || len || payload` 做 FNV-1a）

记录类型：

- `1` = Meta
- `2` = Table Schema
- `3` = Table Segment
- `4` = WAL Record
- `5` = WAL Checkpoint
- `6` = Series Segment（历史单列）
- `7` = Table Index（持久化索引）

### Meta 记录（type = 1）

Payload：

- `magic`：4 bytes = `NTSM`
- `version`：u8 = `1`
- `last_seq`：u64
- `retention_ms`：i64（`-1` 表示未设置）

### Table Schema 记录（type = 2）

Payload：

- `name_len`：u16
- `name_bytes`：`name_len` bytes（UTF-8 表名）
- `schema_bytes`：见 **Schema Payload**

### Table Segment 记录（type = 3）

Payload：

- `name_len`：u16
- `name_bytes`：`name_len` bytes（UTF-8 表名）
- `segment_bytes`：见 **Table Segment Payload**

### WAL Record（type = 4）

Payload：

- WAL 记录字节，以 `NTWL` 开头（与内存 WAL 逻辑布局一致）

### WAL Checkpoint（type = 5）

Payload：

- 空

### Series Segment 记录（type = 6）

Payload：

- `name_len`：u16
- `name_bytes`：`name_len` bytes（UTF-8 序列名）
- `segment_bytes`：见 **Series Segment Payload**

### Table Index 记录（type = 7）

Payload：

- `name_len`：u16
- `name_bytes`：`name_len` bytes（UTF-8 表名）
- `index_bytes`：见 **Table Index Payload**

## Schema Payload

Header：

- `magic`：4 bytes = `NTSC`
- `version`：u8 = `1`
- `ncols`：u16

接着重复 `ncols` 次：

- `name_len`：u16
- `name_bytes`：`name_len` bytes（UTF-8）
- `col_type`：u8
  - `1` = Float64
  - `2` = Int64
  - `3` = Bool
  - `4` = Utf8

说明：

- v1 schema（version=1）仅支持 **Float64** 取值列；`ts_ms` 为隐含列。

## Table Segment Payload（多列）

Segment header：

- `magic`：4 bytes = `NTTB`
- `version`：u8 = `2`
- `min_seq`：u64
- `max_seq`：u64
- `count`：u32（该段行数）
- `min_ts`：i64
- `max_ts`：i64

Extension header（v1+）：

- `ext_len`：u16（bytes）
- `ext_bytes`：`ext_len` bytes（目前为空/预留）

时间戳块：

- `ts_codec`：u8
  - `1` = `TS_CODEC_TS64`（NanoTS 时间戳压缩器）
- `ts_len`：u32（bytes）
- `ts_bytes`：`ts_len` bytes（压缩后的 `i64` 毫秒）

列数据块：

- `ncols`：u16（必须与 schema 一致）

接着重复 `ncols` 次：

- `null_len`：u32（bytes，`0` 表示无 NULL）
- `null_bytes`：`null_len` bytes（有效位图，LSB-first）
- `col_codec`：u8
  - `2` = `TABLE_COL_F64_XOR`（Gorilla XOR 风格）
  - `3` = `TABLE_COL_I64_D2`（`i64` 压缩存储）
  - `4` = `TABLE_COL_BOOL`（bool 位图）
  - `5` = `TABLE_COL_UTF8`（offsets + data）
- `col_len`：u32（bytes）
- `col_bytes`：`col_len` bytes（按 codec 编码）

说明：

- v1 table segment（version=1）没有 null bitmap 字段，仅支持 Float64 列。

## Series Segment Payload（历史单列）

Segment header：

- `magic`：4 bytes = `NTSG`
- `version`：u8 = `1`
- `min_seq`：u64
- `max_seq`：u64
- `count`：u32
- `min_ts`：i64
- `max_ts`：i64

时间戳块：

- `ts_codec`：u8
  - `1` = `TS_CODEC_TS64`
- `ts_len`：u32
- `ts_bytes`：`ts_len` bytes

取值块：

- `val_len`：u32
- `val_bytes`：`val_len` bytes（`count * 8` little-endian `f64`）

说明：

- Series 结构用于向后兼容和 benchmark；内核 API 统一使用 **tables**（包括名为 `value` 的单列表）。

## Table Index Payload

Header：

- `magic`：4 bytes = `NTSI`
- `version`：u8 = `1`
- `entry_count`：u32

接着重复 `entry_count` 次：

- `offset`：u64（segment 在 `.ntt` 内的偏移）
- `len`：u64（segment 字节长度）
- `min_ts`：i64
- `max_ts`：i64
- `min_seq`：u64
- `max_seq`：u64
- `count`：u32（segment 行数）
