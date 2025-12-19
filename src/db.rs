// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::storage::{Storage, TableStats};
use crate::wal::{Wal, WalRecord, WalRecordOwned, WalValue};
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "datafusion")]
use std::sync::Arc;


#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub ts_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct NanoTsOptions {
    pub retention: Option<Duration>,
    pub block_points: usize,
}

impl Default for NanoTsOptions {
    fn default() -> Self {
        Self {
            retention: None,
            block_points: 1024,
        }
    }
}

#[derive(Debug)]
struct Meta {
    last_seq: u64,
    retention_ms: Option<i64>,
}

impl Meta {
    fn new() -> Self {
        Self {
            last_seq: 0,
            retention_ms: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    F64,
    I64,
    Bool,
    Utf8,
}

impl ColumnType {
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::F64),
            2 => Some(Self::I64),
            3 => Some(Self::Bool),
            4 => Some(Self::Utf8),
            _ => None,
        }
    }

    pub fn to_tag(self) -> u8 {
        match self {
            Self::F64 => 1,
            Self::I64 => 2,
            Self::Bool => 3,
            Self::Utf8 => 4,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub col_type: ColumnType,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn f64_columns(columns: &[&str]) -> Self {
        Self {
            columns: columns
                .iter()
                .map(|name| ColumnSchema {
                    name: (*name).to_string(),
                    col_type: ColumnType::F64,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    F64(f64),
    I64(i64),
    Bool(bool),
    Utf8(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    F64(Vec<Option<f64>>),
    I64(Vec<Option<i64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<String>>),
}

fn value_matches_type(value: &Value, col_type: ColumnType) -> bool {
    match (value, col_type) {
        (Value::Null, _) => true,
        (Value::F64(_), ColumnType::F64) => true,
        (Value::I64(_), ColumnType::I64) => true,
        (Value::Bool(_), ColumnType::Bool) => true,
        (Value::Utf8(_), ColumnType::Utf8) => true,
        _ => false,
    }
}

fn schema_matches_f64_columns(schema: &TableSchema, columns: &[&str]) -> bool {
    if schema.columns.len() != columns.len() {
        return false;
    }
    for (col, name) in schema.columns.iter().zip(columns.iter()) {
        if col.col_type != ColumnType::F64 || col.name != *name {
            return false;
        }
    }
    true
}

fn init_column_buffers(schema: &TableSchema, capacity: usize) -> Vec<ColumnData> {
    schema
        .columns
        .iter()
        .map(|col| match col.col_type {
            ColumnType::F64 => ColumnData::F64(Vec::with_capacity(capacity)),
            ColumnType::I64 => ColumnData::I64(Vec::with_capacity(capacity)),
            ColumnType::Bool => ColumnData::Bool(Vec::with_capacity(capacity)),
            ColumnType::Utf8 => ColumnData::Utf8(Vec::with_capacity(capacity)),
        })
        .collect()
}

fn to_wal_value(value: &Value) -> WalValue<'_> {
    match value {
        Value::Null => WalValue::Null,
        Value::F64(v) => WalValue::F64(*v),
        Value::I64(v) => WalValue::I64(*v),
        Value::Bool(v) => WalValue::Bool(*v),
        Value::Utf8(v) => WalValue::Utf8(v.as_str()),
    }
}

#[derive(Debug)]
struct PendingRow {
    seq: u64,
    ts_ms: i64,
    values: Vec<Value>,
}

#[derive(Debug)]
struct TableBuffer {
    schema: TableSchema,
    rows: Vec<PendingRow>,
}

#[derive(Debug)]
pub struct NanoTsDb {
    storage: Storage,
    wal: Wal,
    meta: Meta,
    next_seq: u64,
    options: NanoTsOptions,
    tables: HashMap<String, TableBuffer>,
}

impl NanoTsDb {
    pub fn open(root: impl AsRef<Path>, options: NanoTsOptions) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let storage = Storage::open(&root)?;
        let mut meta = match storage.read_latest_meta()? {
            Some((last_seq, retention_ms)) => Meta {
                last_seq,
                retention_ms,
            },
            None => Meta::new(),
        };
        if options.retention.is_some() {
            meta.retention_ms = options
                .retention
                .map(|d| d.as_millis().min(i64::MAX as u128) as i64);
        }
        let disk_max_seq = storage.scan_max_seq().unwrap_or(0);
        meta.last_seq = meta.last_seq.max(disk_max_seq);

        let wal = Wal::open(&root)?;

        let mut db = Self {
            storage,
            wal,
            meta,
            next_seq: 1,
            options,
            tables: HashMap::new(),
        };

        db.load_table_schemas()?;
        db.replay_wal()?;
        db.next_seq = db.meta.last_seq.saturating_add(1);
        Ok(db)
    }

    pub fn retention(&self) -> Option<Duration> {
        self.meta.retention_ms.map(|ms| Duration::from_millis(ms as u64))
    }

    pub fn append(&mut self, series: &str, ts_ms: i64, value: f64) -> io::Result<u64> {
        // Single-column convenience API mapped to a table with one Float64 column named "value".
        if !self.tables.contains_key(series) && !self.storage.table_exists(series) {
            self.create_table(series, &["value"])?;
        }
        self.append_row(series, ts_ms, &[value])
    }

    pub fn create_table(&mut self, table: &str, columns: &[&str]) -> io::Result<()> {
        if columns.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "table must have at least 1 column",
            ));
        }
        if self.tables.contains_key(table) || self.storage.table_exists(table) {
            // Best-effort: ensure schema matches if it already exists.
            let existing = self.storage.read_table_schema(table)?;
            if !schema_matches_f64_columns(&existing, columns) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "table exists with different schema",
                ));
            }
            return Ok(());
        }

        let schema = TableSchema::f64_columns(columns);
        self.create_table_schema(table, schema)
    }

    pub fn create_table_typed(
        &mut self,
        table: &str,
        columns: &[(&str, ColumnType)],
    ) -> io::Result<()> {
        if columns.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "table must have at least 1 column",
            ));
        }
        if self.tables.contains_key(table) || self.storage.table_exists(table) {
            let existing = self.storage.read_table_schema(table)?;
            if existing.columns.len() != columns.len() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "table exists with different schema",
                ));
            }
            for (col, (name, col_type)) in existing.columns.iter().zip(columns.iter()) {
                if col.name != *name || col.col_type != *col_type {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "table exists with different schema",
                    ));
                }
            }
            return Ok(());
        }

        let schema = TableSchema {
            columns: columns
                .iter()
                .map(|(name, col_type)| ColumnSchema {
                    name: (*name).to_string(),
                    col_type: *col_type,
                })
                .collect(),
        };
        self.create_table_schema(table, schema)
    }

    fn create_table_schema(&mut self, table: &str, schema: TableSchema) -> io::Result<()> {
        self.storage.write_table_schema(table, &schema)?;
        self.tables.insert(
            table.to_string(),
            TableBuffer {
                schema,
                rows: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn append_row(&mut self, table: &str, ts_ms: i64, values: &[f64]) -> io::Result<u64> {
        if !self.tables.contains_key(table) && !self.storage.table_exists(table) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "table not found"));
        }
        if !self.tables.contains_key(table) {
            let schema = self.storage.read_table_schema(table)?;
            self.tables.insert(
                table.to_string(),
                TableBuffer {
                    schema,
                    rows: Vec::new(),
                },
            );
        }
        let typed_values: Vec<Value> = values.iter().copied().map(Value::F64).collect();
        self.append_row_values(table, ts_ms, &typed_values)
    }

    pub fn append_row_typed(
        &mut self,
        table: &str,
        ts_ms: i64,
        values: &[Value],
    ) -> io::Result<u64> {
        if !self.tables.contains_key(table) && !self.storage.table_exists(table) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "table not found"));
        }
        if !self.tables.contains_key(table) {
            let schema = self.storage.read_table_schema(table)?;
            self.tables.insert(
                table.to_string(),
                TableBuffer {
                    schema,
                    rows: Vec::new(),
                },
            );
        }
        self.append_row_values(table, ts_ms, values)
    }

    fn append_row_values(
        &mut self,
        table: &str,
        ts_ms: i64,
        values: &[Value],
    ) -> io::Result<u64> {
        let schema = &self.tables.get(table).unwrap().schema;
        if values.len() != schema.columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "values length mismatch",
            ));
        }
        for (value, col) in values.iter().zip(schema.columns.iter()) {
            if !value_matches_type(value, col.col_type) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "column type mismatch",
                ));
            }
        }

        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);

        if schema.columns.iter().all(|c| c.col_type == ColumnType::F64) {
            let mut raw: Vec<f64> = Vec::with_capacity(values.len());
            for v in values {
                match v {
                    Value::F64(x) => raw.push(*x),
                    Value::Null => raw.push(0.0),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "column type mismatch",
                        ))
                    }
                }
            }
            self.wal.append(WalRecord::AppendRow {
                seq,
                table,
                ts_ms,
                values: &raw,
            })?;
        } else {
            let wal_values = values.iter().map(to_wal_value).collect::<Vec<_>>();
            self.wal.append(WalRecord::AppendRowTyped {
                seq,
                table,
                ts_ms,
                values: &wal_values,
            })?;
        }

        let buf = self.tables.get_mut(table).unwrap();
        buf.rows.push(PendingRow {
            seq,
            ts_ms,
            values: values.to_vec(),
        });

        if buf.rows.len() >= self.options.block_points {
            self.flush_table(table)?;
        }
        Ok(seq)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let tables: Vec<String> = self.tables.keys().cloned().collect();
        for t in tables {
            self.flush_table(&t)?;
        }

        self.wal.flush()?;
        self.wal.reset()?;
        self.storage.write_meta(self.meta.last_seq, self.meta.retention_ms)?;
        for t in self.tables.keys() {
            self.storage.write_table_index(t)?;
        }
        Ok(())
    }

    pub fn query_range(&self, series: &str, start_ms: i64, end_ms: i64) -> io::Result<Vec<Point>> {
        let schema = self.storage.read_table_schema(series)?;
        if schema.columns.len() != 1
            || schema.columns[0].name != "value"
            || schema.columns[0].col_type != ColumnType::F64
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "series query only supports single-column tables named `value`",
            ));
        }

        let (ts, cols) = self.query_table_range_columns(series, start_ms, end_ms)?;
        let mut out = Vec::with_capacity(ts.len());
        for (i, ts_ms) in ts.iter().copied().enumerate() {
            out.push(Point {
                ts_ms,
                value: cols[0][i],
            });
        }
        Ok(out)
    }

    pub fn query_table_range_columns(
        &self,
        table: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> io::Result<(Vec<i64>, Vec<Vec<f64>>)> {
        let (ts_ms, cols) = self.query_table_range_typed(table, start_ms, end_ms)?;
        let mut out_cols: Vec<Vec<f64>> = Vec::with_capacity(cols.len());
        for col in cols {
            match col {
                ColumnData::F64(values) => {
                    let mut out = Vec::with_capacity(values.len());
                    for v in values {
                        match v {
                            Some(x) => out.push(x),
                            None => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "null value in f64 query result",
                                ))
                            }
                        }
                    }
                    out_cols.push(out);
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "query_table_range_columns requires Float64 columns",
                    ))
                }
            }
        }
        Ok((ts_ms, out_cols))
    }

    pub fn query_table_range_typed(
        &self,
        table: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> io::Result<(Vec<i64>, Vec<ColumnData>)> {
        let schema = self.storage.read_table_schema(table)?;
        let mut out = self
            .storage
            .read_table_columns_in_range(table, &schema, start_ms, end_ms)?;

        if let Some(buf) = self.tables.get(table) {
            for r in &buf.rows {
                if r.ts_ms < start_ms || r.ts_ms > end_ms {
                    continue;
                }
                out.0.push(r.ts_ms);
                for (cidx, value) in r.values.iter().enumerate() {
                    match (&mut out.1[cidx], value) {
                        (ColumnData::F64(col), Value::F64(v)) => col.push(Some(*v)),
                        (ColumnData::F64(col), Value::Null) => col.push(None),
                        (ColumnData::I64(col), Value::I64(v)) => col.push(Some(*v)),
                        (ColumnData::I64(col), Value::Null) => col.push(None),
                        (ColumnData::Bool(col), Value::Bool(v)) => col.push(Some(*v)),
                        (ColumnData::Bool(col), Value::Null) => col.push(None),
                        (ColumnData::Utf8(col), Value::Utf8(v)) => col.push(Some(v.clone())),
                        (ColumnData::Utf8(col), Value::Null) => col.push(None),
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "buffer value type mismatch",
                            ))
                        }
                    }
                }
            }
        }

        let mut idx: Vec<usize> = (0..out.0.len()).collect();
        idx.sort_by_key(|&i| out.0[i]);
        let mut ts_sorted = Vec::with_capacity(out.0.len());
        let mut cols_sorted: Vec<ColumnData> = out
            .1
            .iter()
            .map(|col| match col {
                ColumnData::F64(_) => ColumnData::F64(Vec::with_capacity(out.0.len())),
                ColumnData::I64(_) => ColumnData::I64(Vec::with_capacity(out.0.len())),
                ColumnData::Bool(_) => ColumnData::Bool(Vec::with_capacity(out.0.len())),
                ColumnData::Utf8(_) => ColumnData::Utf8(Vec::with_capacity(out.0.len())),
            })
            .collect();
        for i in idx {
            ts_sorted.push(out.0[i]);
            for (cidx, col) in out.1.iter().enumerate() {
                match (&mut cols_sorted[cidx], col) {
                    (ColumnData::F64(dst), ColumnData::F64(src)) => dst.push(src[i]),
                    (ColumnData::I64(dst), ColumnData::I64(src)) => dst.push(src[i]),
                    (ColumnData::Bool(dst), ColumnData::Bool(src)) => dst.push(src[i]),
                    (ColumnData::Utf8(dst), ColumnData::Utf8(src)) => dst.push(src[i].clone()),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "column type mismatch",
                        ))
                    }
                }
            }
        }
        Ok((ts_sorted, cols_sorted))
    }

    pub fn table_schema(&self, table: &str) -> io::Result<TableSchema> {
        self.storage.read_table_schema(table)
    }

    pub fn table_stats(&self, table: &str) -> io::Result<TableStats> {
        self.storage.table_stats(table)
    }

    pub fn table_time_range(&self, table: &str) -> io::Result<Option<(i64, i64)>> {
        self.storage.table_time_range(table)
    }

    pub fn list_tables(&self) -> io::Result<Vec<String>> {
        self.storage.list_tables()
    }

    pub fn last_seq(&self) -> u64 {
        self.meta.last_seq
    }

    #[cfg(feature = "datafusion")]
    /// Execute a read-only SQL query via DataFusion.
    ///
    /// Requires the `datafusion` feature and runs in a single-threaded Tokio runtime.
    pub fn query_sql(
        self: &Arc<Self>,
        sql: &str,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        crate::datafusion::query_sql(self.clone(), sql)
    }

    pub fn compact_retention_now(&mut self) -> io::Result<()> {
        let retention_ms = match self.meta.retention_ms {
            Some(ms) => ms,
            None => return Ok(()),
        };
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let cutoff = now_ms.saturating_sub(retention_ms);
        self.compact_retention(cutoff)
    }

    pub fn compact_retention(&mut self, cutoff_ms: i64) -> io::Result<()> {
        // Flush pending points first, so retention affects a stable on-disk view.
        self.flush()?;
        for table in self.storage.list_tables()? {
            self.storage.compact_table_retention(&table, cutoff_ms)?;
        }
        Ok(())
    }

    #[cfg(feature = "arrow")]
    pub fn query_range_arrow(
        &self,
        series: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> io::Result<crate::arrow::ArrowBatch> {
        let (ts, cols) = self.query_table_range_typed(series, start_ms, end_ms)?;
        self.export_table_arrow(series, ts, cols)
    }

    #[cfg(feature = "arrow")]
    pub fn query_table_range_arrow(
        &self,
        table: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> io::Result<crate::arrow::ArrowBatch> {
        let (ts, cols) = self.query_table_range_typed(table, start_ms, end_ms)?;
        self.export_table_arrow(table, ts, cols)
    }

    fn replay_wal(&mut self) -> io::Result<()> {
        let last = self.meta.last_seq;
        let mut max_seen = last;

        let mut records: Vec<WalRecordOwned> = Vec::new();
        self.wal.replay(|r| {
            records.push(r);
            Ok(())
        })?;

        for r in records {
            match r {
                WalRecordOwned::Append {
                    seq,
                    series,
                    ts_ms,
                    value,
                } => {
                    if seq <= last {
                        continue;
                    }
                    max_seen = max_seen.max(seq);
                    if !self.tables.contains_key(&series) && !self.storage.table_exists(&series) {
                        self.create_table(&series, &["value"])?;
                    }
                    let buf = self.tables.get_mut(&series).unwrap();
                    buf.rows.push(PendingRow {
                        seq,
                        ts_ms,
                        values: vec![Value::F64(value)],
                    });
                }
                WalRecordOwned::AppendRow {
                    seq,
                    table,
                    ts_ms,
                    values,
                } => {
                    if seq <= last {
                        continue;
                    }
                    max_seen = max_seen.max(seq);
                    if !self.tables.contains_key(&table) && !self.storage.table_exists(&table) {
                        return Err(io::Error::new(io::ErrorKind::NotFound, "table not found"));
                    }
                    if !self.tables.contains_key(&table) {
                        let schema = self.storage.read_table_schema(&table)?;
                        self.tables.insert(
                            table.to_string(),
                            TableBuffer {
                                schema,
                                rows: Vec::new(),
                            },
                        );
                    }
                    let buf = self.tables.get_mut(&table).unwrap();
                    if values.len() != buf.schema.columns.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "WAL row schema mismatch",
                        ));
                    }
                    let typed = values.into_iter().map(Value::F64).collect();
                    buf.rows.push(PendingRow {
                        seq,
                        ts_ms,
                        values: typed,
                    });
                }
                WalRecordOwned::AppendRowTyped {
                    seq,
                    table,
                    ts_ms,
                    values,
                } => {
                    if seq <= last {
                        continue;
                    }
                    max_seen = max_seen.max(seq);
                    if !self.tables.contains_key(&table) && !self.storage.table_exists(&table) {
                        return Err(io::Error::new(io::ErrorKind::NotFound, "table not found"));
                    }
                    if !self.tables.contains_key(&table) {
                        let schema = self.storage.read_table_schema(&table)?;
                        self.tables.insert(
                            table.to_string(),
                            TableBuffer {
                                schema,
                                rows: Vec::new(),
                            },
                        );
                    }
                    let buf = self.tables.get_mut(&table).unwrap();
                    if values.len() != buf.schema.columns.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "WAL row schema mismatch",
                        ));
                    }
                    buf.rows.push(PendingRow {
                        seq,
                        ts_ms,
                        values,
                    });
                }
            }
        }

        self.meta.last_seq = self.meta.last_seq.max(max_seen);
        Ok(())
    }

    fn flush_table(&mut self, table: &str) -> io::Result<()> {
        let Some(buf) = self.tables.get_mut(table) else {
            return Ok(());
        };
        if buf.rows.is_empty() {
            return Ok(());
        }

        let schema = buf.schema.clone();
        let mut start = 0usize;
        while start < buf.rows.len() {
            let end = (start + self.options.block_points).min(buf.rows.len());
            let block = &buf.rows[start..end];
            let min_seq = block.first().unwrap().seq;
            let max_seq = block.last().unwrap().seq;
            let mut ts: Vec<i64> = Vec::with_capacity(block.len());
            let mut cols = init_column_buffers(&schema, block.len());
            for r in block {
                ts.push(r.ts_ms);
                for (i, v) in r.values.iter().enumerate() {
                    match (&mut cols[i], v) {
                        (ColumnData::F64(col), Value::F64(x)) => col.push(Some(*x)),
                        (ColumnData::F64(col), Value::Null) => col.push(None),
                        (ColumnData::I64(col), Value::I64(x)) => col.push(Some(*x)),
                        (ColumnData::I64(col), Value::Null) => col.push(None),
                        (ColumnData::Bool(col), Value::Bool(x)) => col.push(Some(*x)),
                        (ColumnData::Bool(col), Value::Null) => col.push(None),
                        (ColumnData::Utf8(col), Value::Utf8(x)) => col.push(Some(x.clone())),
                        (ColumnData::Utf8(col), Value::Null) => col.push(None),
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "column type mismatch",
                            ))
                        }
                    }
                }
            }
            self.storage
                .append_table_segment(table, &schema, &ts, &cols, min_seq, max_seq)?;
            self.meta.last_seq = self.meta.last_seq.max(max_seq);
            start = end;
        }
        buf.rows.clear();
        Ok(())
    }

    fn load_table_schemas(&mut self) -> io::Result<()> {
        for table in self.storage.list_tables()? {
            let schema = self.storage.read_table_schema(&table)?;
            self.tables
                .entry(table)
                .or_insert(TableBuffer { schema, rows: Vec::new() });
        }
        Ok(())
    }

    #[cfg(feature = "arrow")]
    fn export_table_arrow(
        &self,
        table: &str,
        ts: Vec<i64>,
        cols: Vec<ColumnData>,
    ) -> io::Result<crate::arrow::ArrowBatch> {
        let schema = self.storage.read_table_schema(table)?;
        if cols.len() != schema.columns.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "schema mismatch"));
        }
        let mut columns: Vec<(&str, ColumnData)> = Vec::with_capacity(schema.columns.len());
        for (col_schema, col) in schema.columns.iter().zip(cols) {
            columns.push((col_schema.name.as_str(), col));
        }
        crate::arrow::ArrowBatch::from_ts_columns(table, ts, columns)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "arrow export failed"))
    }

    pub fn pack_table(&mut self, table: &str, target_segment_points: usize) -> io::Result<()> {
        self.flush()?;
        self.storage.pack_table(table, target_segment_points)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbfile;
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    fn fresh_db_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nanots_test_{}_{}_{}.ntt",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    #[test]
    fn test_table_append_flush_reopen_query() {
        let path = fresh_db_path("table_roundtrip");
        let opts = NanoTsOptions {
            retention: Some(Duration::from_secs(3600)),
            ..Default::default()
        };
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table("sensor", &["temp", "humidity"]).unwrap();
            for i in 0..1000i64 {
                db.append_row("sensor", 1000 + i, &[25.0 + (i as f64) * 0.01, 60.0])
                    .unwrap();
            }
            db.flush().unwrap();
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let (ts, cols) = db.query_table_range_columns("sensor", 1200, 1300).unwrap();
            assert!(!ts.is_empty());
            assert_eq!(cols.len(), 2);
            assert_eq!(ts.len(), cols[0].len());
            assert_eq!(ts.len(), cols[1].len());
            assert!(ts[0] >= 1200 && *ts.last().unwrap() <= 1300);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_pack_table() {
        let path = fresh_db_path("pack");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table("t", &["a", "b"]).unwrap();
            for i in 0..5000i64 {
                db.append_row("t", 10_000 + i, &[i as f64, (i as f64) * 0.5])
                    .unwrap();
            }
            db.flush().unwrap();
            db.pack_table("t", 512).unwrap();
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let (ts, cols) = db.query_table_range_columns("t", 10_000, 20_000).unwrap();
            assert_eq!(ts.len(), cols[0].len());
            assert_eq!(ts.len(), cols[1].len());
            assert!(ts.len() >= 5000);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_wal_replay_after_crash() {
        let path = fresh_db_path("wal_replay");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table("sensor", &["temp"]).unwrap();
            db.append_row("sensor", 1000, &[1.0]).unwrap();
            db.append_row("sensor", 1001, &[2.0]).unwrap();
            // Drop without flush to force WAL replay on reopen.
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let (ts, cols) = db.query_table_range_columns("sensor", 0, 10_000).unwrap();
            assert_eq!(cols.len(), 1);
            assert_eq!(ts.len(), 2);
            assert_eq!(cols[0].len(), 2);
            assert_eq!(ts[0], 1000);
            assert_eq!(ts[1], 1001);
            assert_eq!(cols[0][0], 1.0);
            assert_eq!(cols[0][1], 2.0);
        }
        let _ = fs::remove_file(path);
    }

    fn count_records(path: &Path, record_type: u8) -> io::Result<usize> {
        let mut count = 0usize;
        dbfile::iter_records(path, |hdr, _| {
            if hdr.record_type == record_type {
                count += 1;
            }
            Ok(())
        })?;
        Ok(count)
    }

    #[test]
    fn test_pack_then_wal_replay_after_crash() {
        let path = fresh_db_path("pack_wal_replay");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table("sensor", &["temp"]).unwrap();
            for i in 0..1000i64 {
                db.append_row("sensor", 1000 + i, &[i as f64]).unwrap();
            }
            db.flush().unwrap();
            db.pack_table("sensor", 256).unwrap();
            let wal_count = count_records(&path, dbfile::RECORD_WAL).unwrap();
            assert_eq!(wal_count, 0);
            let chk_count = count_records(&path, dbfile::RECORD_WAL_CHECKPOINT).unwrap();
            assert_eq!(chk_count, 0);

            db.append_row("sensor", 3000, &[42.0]).unwrap();
            db.append_row("sensor", 3001, &[43.0]).unwrap();
            // Drop without flush to force WAL replay.
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let (ts, cols) = db.query_table_range_columns("sensor", 0, 10_000).unwrap();
            assert_eq!(cols.len(), 1);
            assert!(ts.contains(&3000));
            assert!(ts.contains(&3001));
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_flush_writes_table_index() {
        let path = fresh_db_path("index_record");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table("t", &["a"]).unwrap();
            for i in 0..10i64 {
                db.append_row("t", 1_000 + i, &[i as f64]).unwrap();
            }
            db.flush().unwrap();
        }
        let idx_count = count_records(&path, dbfile::RECORD_TABLE_INDEX).unwrap();
        assert_eq!(idx_count, 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_typed_table_roundtrip_with_nulls() {
        let path = fresh_db_path("typed_nulls");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table_typed(
                "t",
                &[
                    ("a", ColumnType::I64),
                    ("b", ColumnType::Bool),
                    ("c", ColumnType::Utf8),
                    ("d", ColumnType::F64),
                ],
            )
            .unwrap();
            db.append_row_typed(
                "t",
                1000,
                &[
                    Value::I64(42),
                    Value::Bool(true),
                    Value::Utf8("x".to_string()),
                    Value::F64(1.25),
                ],
            )
            .unwrap();
            db.append_row_typed(
                "t",
                1001,
                &[
                    Value::Null,
                    Value::Bool(false),
                    Value::Null,
                    Value::F64(2.5),
                ],
            )
            .unwrap();
            db.flush().unwrap();
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let (ts, cols) = db.query_table_range_typed("t", 0, 10_000).unwrap();
            assert_eq!(ts.len(), 2);
            match &cols[0] {
                ColumnData::I64(values) => {
                    assert_eq!(values[0], Some(42));
                    assert_eq!(values[1], None);
                }
                _ => panic!("unexpected column type"),
            }
            match &cols[1] {
                ColumnData::Bool(values) => {
                    assert_eq!(values[0], Some(true));
                    assert_eq!(values[1], Some(false));
                }
                _ => panic!("unexpected column type"),
            }
            match &cols[2] {
                ColumnData::Utf8(values) => {
                    assert_eq!(values[0].as_deref(), Some("x"));
                    assert_eq!(values[1], None);
                }
                _ => panic!("unexpected column type"),
            }
            match &cols[3] {
                ColumnData::F64(values) => {
                    assert_eq!(values[0], Some(1.25));
                    assert_eq!(values[1], Some(2.5));
                }
                _ => panic!("unexpected column type"),
            }
        }
        let _ = fs::remove_file(path);
    }
}
