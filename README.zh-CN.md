# NanoTS（社区版）

一个用 Rust 编写的**可嵌入**、**仅追加**、**高性能**时序存储内核。

- **许可证**：AGPL-3.0-or-later
- **定位**：类似 SQLite 的 *进程内* TSDB 内核（单机、边缘/嵌入式）
- **文档**：English（[README.md](README.md)、[CONTRIBUTING.md](CONTRIBUTING.md)、[SPEC.md](SPEC.md)），中文（[README.zh-CN.md](README.zh-CN.md)、[CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md)、[SPEC.zh-CN.md](SPEC.zh-CN.md)）

## 状态与兼容性

当前为未发布状态（pre-release），磁盘格式可能随时变更。

## 基准测试（本地、合成数据）

以下结果来自本地运行，数据为合成/理想数据（完美/平滑模式）。仅作方向性参考。

| 测试 | 数据集 | 命令 | 结果 |
| --- | --- | --- | --- |
| 压缩体积 | 10 万点，单列 `f64`，单表 | `python test_final.py` | `.ntt` 体积 5612.29 KB（pack 前），pack 后 624.74 KB；原始约 800 KB |
| 压缩比 | 10 万点，单表 | `python test_compression_ratio.py` | raw_total 1562.50 KB, on_disk 19.57 KB, ratio 79.82x |
| 读性能 | 10 万点，单表 | `python test_read_perf.py` | avg 15.58 ms, p50 14.48 ms, p90 18.91 ms, p99 21.19 ms, 6,420,029 rows/s |

## 你能得到什么

- **进程内**库 API（不内置 HTTP 服务）
- **单文件 DB**（可分享 `.ntt` 文件）
- **Append-only 存储 + WAL + 崩溃恢复**
- **列式表**（v0：隐含 `ts_ms`，取值列仅 `f64`）
- **Retention/TTL**：通过手动 compaction
- **自适应值压缩**
  - `col_codec=2`：浮点 Gorilla XOR
  - `col_codec=3`：当浮点列值是精确整数时自动回退 → 用 NanoTS 整数压缩器编码为 `i64`
- **mmap 读路径**（更少 syscalls，更友好的 OS 页缓存）
- **Apache Arrow C Data Interface** 导出（零拷贝互操作）
- **Python 绑定**（PyO3/maturin），支持 Arrow capsule 导入
- **可配置的记录大小上限**：`NANOTS_MAX_RECORD_SIZE`（默认 64MB）

## 非目标（社区版）

- Clustering / Raft
- 自研 SQL 解析器（如需可用 DataFusion 等外部引擎）
- 内置仪表盘 / Web UI（用 Grafana）
- 内核层的 Auth/RBAC

## 仓库结构

- Rust 内核：`src/`（crate 名：`nanots`）
- C 头文件：`include/nanots.h`
- Python 绑定 crate：`python/nanots-py/`
- 文件格式规范：[SPEC.md](SPEC.md)

## 数据模型

- 一个 **table** 有隐含时间列 `ts_ms: i64`（毫秒）。
- 每个表有 **N 个取值列**，支持多种类型：
  - `F64` (Float64)
  - `I64` (Int64)
  - `Bool` (Boolean)
  - `Utf8` (String/UTF-8)
- 数据按行追加：`append_row(table, ts_ms, values[])`。

说明：

- `append(series, ts_ms, value)` 是便捷 API，用于写单列表。
- Schema 存在 `.ntt` 文件中（见 [SPEC.md](SPEC.md)）。

## SQL（DataFusion 集成）

- 通过 DataFusion 引擎提供完整 SQL 支持（只读查询）
- 支持语句：`CREATE TABLE`、`INSERT INTO ... VALUES ...`、`SELECT`、`WHERE`、`GROUP BY` 等
- 内置 UDF：`time_bucket(interval, timestamp)`、`delta(column)`、`rate(column)`
- 标识符可以不加引号，或使用双引号/反引号（例如：`"my_table"`、`` `my_table` ``）。

## Rust 用法（详细）

### 作为依赖引入（本地路径）

本仓库未发布到 crates.io。在你的项目中：

```toml
[dependencies]
nanots = { path = "/path/to/ts_compressor_rust", features = ["arrow"] }
```

### 基础写入与 flush

```rust
use nanots::{NanoTsDb, NanoTsOptions};

let mut db = NanoTsDb::open("data/nanots.ntt", NanoTsOptions::default())?;
db.create_table("sensor", &["temp", "humidity"])?;
db.append_row("sensor", 1704067200000, &[25.5, 60.0])?;
db.flush()?;
# Ok::<(), std::io::Error>(())
```

### 多列、范围查询、retention、手动 pack

```rust
use nanots::{NanoTsDb, NanoTsOptions};
use std::time::Duration;

let opts = NanoTsOptions {
    retention: Some(Duration::from_secs(3600)), // 1 小时
    ..Default::default()
};
let mut db = NanoTsDb::open("data/nanots.ntt", opts)?;

db.create_table("sensor", &["temp", "humidity"])?;

let t0 = 1704067200000i64;
for i in 0..10_000i64 {
    let ts = t0 + i * 1000;
    db.append_row("sensor", ts, &[25.0 + (i as f64) * 0.001, 60.0])?;
}

db.flush()?;

let (ts_ms, cols) = db.query_table_range_columns("sensor", t0, t0 + 60_000)?;
assert_eq!(cols.len(), 2);
println!("rows={}", ts_ms.len());

db.compact_retention_now()?;
db.pack_table("sensor", 8192)?;

# Ok::<(), std::io::Error>(())
```

### 自动维护（后台 pack + retention）

```rust
use nanots::{AutoMaintenanceOptions, NanoTsDbShared, NanoTsOptions};
use std::time::Duration;

let opts = NanoTsOptions {
    retention: Some(Duration::from_secs(3600)),
    auto_maintenance: Some(AutoMaintenanceOptions::default()),
    ..Default::default()
};
let db = NanoTsDbShared::open("data/nanots.ntt", opts)?;
```

### Rust API 一览（快速参考）

- `NanoTsDb::open(path, opts)`：打开/创建 DB 目录
- `create_table(table, columns)`：创建表 schema
- `append_row(table, ts_ms, values)`：追加一行（先写 WAL）
- `append(series, ts_ms, value)`：便捷单列写入
- `flush()`：将缓冲区落盘到 `.ntt`，并 checkpoint + 截断 WAL
- `query_table_range_columns(table, start, end)`：返回 `(ts_ms, cols)`
- `pack_table(table, target_segment_points)`：手动 compaction（重写 segment）
- `compact_retention_now()`：删除 retention 之前的点

## Python 用法（详细）

### 构建并安装（开发模式）

要求：Rust toolchain + Python + `maturin`。

```bash
cd python/nanots-py
maturin develop --release
```

该命令会将本地 `nanots_db` 扩展模块安装到当前 Python 环境。
类型提示位于 `python/nanots-py/nanots_db.pyi`。

测试安装：
```bash
python -c "import nanots_db; print(nanots_db.__version__)"
```

### 基础写入与 flush

```python
import nanots_db

db = nanots_db.Db("./my_db.ntt", 3600 * 1000)  # retention: 1 小时（毫秒）
db.append("sensor_x", 1704067200000, 25.5)
db.flush()
```

### 多列表

```python
import nanots_db

db = nanots_db.Db("./my_db.ntt")
db.create_table("sensor", ["temp", "humidity"])

t0 = 1704067200000
for i in range(10000):
    ts = t0 + i * 1000
    db.append_row("sensor", ts, [25.0 + i * 0.001, 60.0])

db.flush()
ts_ms, cols = db.query_table_range_columns("sensor", t0, t0 + 60000)
print("rows:", len(ts_ms), "cols:", len(cols))
```

### 类型化表（多数据类型）

```python
import nanots_db

db = nanots_db.Db("./my_db.ntt")
# 创建混合类型表
db.create_table_typed("events", [
    ("sensor_id", "i64"),
    ("temperature", "f64"),
    ("status", "bool"),
    ("message", "utf8")
])

# 追加类型化数据
db.append_row_typed("events", 1704067200000, [
    1001,           # sensor_id (i64)
    25.5,           # temperature (f64)
    True,           # status (bool)
    "OK"            # message (utf8)
])
db.flush()

# 查询类型化数据
ts_ms, cols = db.query_table_range_typed("events", 0, 10**18)
print(f"行数: {len(ts_ms)}")
print(f"传感器 ID: {cols[0][:5]}")  # i64 列
print(f"温度: {cols[1][:5]}")  # f64 列
```

### Arrow 零拷贝到 PyArrow / Pandas

```python
import nanots_db
import pyarrow.lib

db = nanots_db.Db("./my_db.ntt")
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", 0, 10**18)
batch = pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)

df = batch.to_pandas()  # 在 Arrow 侧尽可能零拷贝
print(df.head())
```

### Arrow 批量写入（来自 PyArrow）

```python
import nanots_db
import pyarrow as pa

db = nanots_db.Db("./my_db.ntt")
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

### C API 批量写入（行主序）

```c
#include "nanots.h"

int main(void) {
  NanotsHandle* db = nanots_open("./my_db.ntt", -1);
  const char* cols[] = {"a", "b"};
  nanots_create_table(db, "t", cols, 2);

  int64_t ts[] = {1000, 1001, 1002};
  double vals[] = {
      1.0, 2.0,
      3.0, 4.0,
      5.0, 6.0,
  };
  nanots_append_rows(db, "t", ts, 3, vals, 2);
  nanots_flush(db);
  nanots_close(db);
  return 0;
}
```

### Stats（空间统计与 codec 验证）

```python
import nanots_db

db = nanots_db.Db("./my_db.ntt")
print(db.stats("sensor"))
```

### 手动 pack / compaction

```python
import nanots_db

db = nanots_db.Db("./my_db.ntt")
db.pack_table("sensor", 8192)
```

### SQL 查询（DataFusion 集成）

```python
import nanots_db

db = nanots_db.Db("./my_db.ntt")

# 同步 SQL 到 Pandas
df = db.query_sql_to_pandas("SELECT * FROM sensor WHERE ts_ms > 1704067200000")
print(df.head())

# 同步 SQL 到 Polars
df_pl = db.query_sql_to_polars("SELECT time_bucket(60000, ts_ms) as bucket, AVG(temp) as avg_temp FROM sensor GROUP BY bucket ORDER BY bucket")
print(df_pl)

# 异步 SQL 查询
import asyncio

async def query_async():
    capsule = await db.query_sql_async("SELECT * FROM events WHERE status = true")
    # 处理 Arrow stream capsule...
    pass

asyncio.run(query_async())

# 直接消费 Arrow stream
capsule = db.query_sql_arrow_stream_capsule("SELECT COUNT(*) FROM sensor")
import pyarrow
reader = pyarrow.RecordBatchReader._import_from_c_capsule(capsule)
for batch in reader:
    print(batch.to_pandas())
```

### 数据库诊断

```python
import nanots_db

db = nanots_db.Db("./my_db.ntt")

# 获取表统计信息
stats = db.stats("sensor")
print(f"行数: {stats['rows']}")
print(f"压缩比: {stats['compression_ratio_total_x']:.2f}x")

# 获取数据库诊断信息
diag = db.diagnose()
print(f"WAL 大小: {diag['wal_bytes']} 字节")
print(f"WAL 比率: {diag['wal_ratio']:.2f}")
print(f"待处理行数: {diag['pending_rows']}")

# 获取表时间范围
time_range = db.table_time_range("sensor")
if time_range:
    print(f"时间范围: {time_range[0]} - {time_range[1]}")
```

## 文件格式

详见 [SPEC.md](SPEC.md) 中的单文件记录布局与各 payload 格式。

## 构建与测试

内核测试（可离线运行）：

```bash
cargo test --offline --features arrow
```
