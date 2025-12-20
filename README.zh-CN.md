# NanoTS（社区版）

一个用 Rust 编写的**可嵌入（In-Process）**、**仅追加（Append-only）**、**高性能**的时序存储内核。

- **许可证**：AGPL-3.0-or-later
- **定位**：类似 SQLite 的单机/边缘侧 TSDB Kernel（不是分布式集群）
- **文档**：English（[README.md](README.md)、[CONTRIBUTING.md](CONTRIBUTING.md)、[SPEC.md](SPEC.md)）、中文（[README.zh-CN.md](README.zh-CN.md)、[CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md)、[SPEC.zh-CN.md](SPEC.zh-CN.md)）

## 状态与兼容性

当前为未发布状态（pre-release），磁盘格式可能随时调整。

## 本地测评（合成数据）

以下结果来自本地运行，数据为合成/理想数据（规律性很强）。仅作方向性参考。

| 测试 | 数据集 | 命令 | 结果 |
| --- | --- | --- | --- |
| 压缩体积 | 10 万点，单列 `f64`，单表 | `python test_final.py` | `.ntt` 体积 5612.29 KB（pack 前），pack 后 624.74 KB；原始约 800 KB |
| 压缩比 | 10 万点，单表 | `python test_compression_ratio.py` | raw_total 1562.50 KB, on_disk 19.57 KB, ratio 79.82x |
| 读性能 | 10 万点，单表 | `python test_read_perf.py` | avg 15.58 ms, p50 14.48 ms, p90 18.91 ms, p99 21.19 ms, 6,420,029 rows/s |


## 你能得到什么

- **In-process**：像 SQLite 一样嵌入进宿主进程，不内置 HTTP Server
- **单文件数据库**（可直接分享 `.ntt` 文件）
- **Append-only + WAL + 崩溃恢复**
- **列式表（v0）**：隐含 `ts_ms`，取值列目前仅支持 `f64`
- **Retention/TTL**：通过手动 compaction/pack 做过期清理
- **自适应值压缩（无损）**
  - `col_codec=2`：`f64` Gorilla XOR
  - `col_codec=3`：自动探测“浮点其实是整数”→ 转 `i64` 走 NanoTS 整数压缩器
- **mmap 读路径**：读 `.ntt` 用 `mmap` + slice 解析，减少 syscalls，充分利用 OS page cache
- **Arrow 零拷贝导出**：Apache Arrow C Data Interface
- **Python 绑定**：PyO3/maturin，支持 Arrow capsule 导入
- **记录大小上限可配置**：环境变量 `NANOTS_MAX_RECORD_SIZE`（默认 64MB）

## 非目标（社区版）

- 不做分布式/集群（Raft、复制等）
- 不自研 SQL 语法（需要 SQL 可外接 DataFusion）
- 不内置 Web UI（建议 Grafana）
- 内核不做鉴权/RBAC

## 仓库结构

- Rust 内核：`src/`（crate 名：`nanots`）
- C 头文件：`include/nanots.h`
- Python 绑定（独立 crate）：`python/nanots-py/`
- 文件格式规范：[SPEC.md](SPEC.md)

## 数据模型

- 一个 **Table** = 隐含时间列 `ts_ms: i64`（毫秒） + N 个 `f64` 取值列。
- 按行追加写：`append_row(table, ts_ms, values[])`。

说明：

- `append(series, ts_ms, value)` 是便捷接口，本质是写单列 table。
- Schema 写入 `.ntt` 单文件中（见 [SPEC.md](SPEC.md)）。

## SQL（简化）

- 支持语句：`CREATE TABLE`、`INSERT INTO ... VALUES ...`
- 标识符支持不加引号，或使用双引号/反引号（例如 `"my_table"`、`` `my_table` ``）。

## Rust 用法（详细）

### 作为依赖引入（本地 path）

本仓库未发布到 crates.io，可用 path 依赖：

```toml
[dependencies]
nanots = { path = "/path/to/ts_compressor_rust", features = ["arrow"] }
```

### 最小示例：写入 + flush

```rust
use nanots::{NanoTsDb, NanoTsOptions};

let mut db = NanoTsDb::open("data/nanots.ntt", NanoTsOptions::default())?;
db.create_table("sensor", &["temp", "humidity"])?;
db.append_row("sensor", 1704067200000, &[25.5, 60.0])?;
db.flush()?;
# Ok::<(), std::io::Error>(())
```

### 完整示例：多列 + 查询 + retention + 手动 pack

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
println!("rows={}", ts_ms.len());
assert_eq!(cols.len(), 2);

db.compact_retention_now()?;
db.pack_table("sensor", 8192)?;

# Ok::<(), std::io::Error>(())
```

### 自动维护（后台 Pack + Retention）

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

### Rust 接口速查

- `NanoTsDb::open(path, opts)`：打开/创建 DB 目录
- `create_table(table, columns)`：创建表 schema
- `append_row(table, ts_ms, values)`：追加一行（先写 WAL）
- `append(series, ts_ms, value)`：便捷单列写入
- `flush()`：落盘为 `.ntt` 并截断 WAL
- `query_table_range_columns(table, start, end)`：返回 `(ts_ms, cols)`
- `pack_table(table, target_segment_points)`：手动 compaction（重写 segments）
- `compact_retention_now()`：按 retention 删除过期数据

## Python 用法（详细）

### 构建并安装到当前环境（开发模式）

要求：Rust toolchain + Python + `maturin`。

```bash
cd python/nanots-py
maturin develop --release
```

该命令会把本地 `nanots` 扩展模块安装到当前 Python 环境。
类型提示位于 `python/nanots-py/nanots.pyi`。

### 最小示例：写入 + flush

```python
import nanots

db = nanots.Db("./my_db.ntt", 3600 * 1000)  # retention: 1 小时（毫秒）
db.append("sensor_x", 1704067200000, 25.5)
db.flush()
```

### 多列表：创建 + 写入 + 查询

```python
import nanots

db = nanots.Db("./my_db.ntt")
db.create_table("sensor", ["temp", "humidity"])

t0 = 1704067200000
for i in range(10000):
    ts = t0 + i * 1000
    db.append_row("sensor", ts, [25.0 + i * 0.001, 60.0])

db.flush()
ts_ms, cols = db.query_table_range_columns("sensor", t0, t0 + 60000)
print("rows:", len(ts_ms), "cols:", len(cols))
```

### Arrow 零拷贝导入到 PyArrow / Pandas

```python
import nanots
import pyarrow.lib

db = nanots.Db("./my_db.ntt")
schema_capsule, array_capsule = db.query_table_range_arrow_capsules("sensor", 0, 10**18)
batch = pyarrow.lib.RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)

df = batch.to_pandas()
print(df.head())
```

### Stats：空间占用与 codec 验证

```python
import nanots

db = nanots.Db("./my_db.ntt")
print(db.stats("sensor"))
```

### 手动 pack / compaction

```python
import nanots

db = nanots.Db("./my_db.ntt")
db.pack_table("sensor", 8192)
```

## 文件格式

详见 [SPEC.md](SPEC.md) / [SPEC.zh-CN.md](SPEC.zh-CN.md)，包含单文件记录布局与各 payload 格式。

## 构建与测试

内核测试（离线也能跑）：

```bash
cargo test --offline --features arrow
```
