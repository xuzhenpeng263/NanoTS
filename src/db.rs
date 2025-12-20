// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::storage::{Storage, TableStats};
use crate::wal::{Wal, WalRecord, WalRecordOwned, WalValue};
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};


#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub ts_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct NanoTsOptions {
    pub retention: Option<Duration>,
    pub block_points: usize,
    pub auto_maintenance: Option<AutoMaintenanceOptions>,
}

impl Default for NanoTsOptions {
    fn default() -> Self {
        Self {
            retention: None,
            block_points: 1024,
            auto_maintenance: Some(AutoMaintenanceOptions::default()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutoMaintenanceOptions {
    pub check_interval: Duration,
    pub retention_check_interval: Duration,
    pub wal_target_ratio: u64,
    pub wal_min_bytes: u64,
    pub target_segment_points: usize,
    pub max_writes_per_sec: u64,
    pub write_load_window: Duration,
    pub min_idle_time: Duration,
    pub max_segments_per_pack: usize,
}

impl Default for AutoMaintenanceOptions {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            retention_check_interval: Duration::from_secs(60),
            wal_target_ratio: 2,
            wal_min_bytes: 32 * 1024 * 1024,
            target_segment_points: 8192,
            max_writes_per_sec: 100_000,
            write_load_window: Duration::from_secs(5),
            min_idle_time: Duration::from_secs(1),
            max_segments_per_pack: 64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnPredicateOp {
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Clone)]
pub enum ColumnPredicateValue {
    F64(f64),
    I64(i64),
    Bool(bool),
    Utf8(String),
}

#[derive(Debug, Clone)]
pub struct ColumnPredicate {
    pub column: String,
    pub op: ColumnPredicateOp,
    pub value: ColumnPredicateValue,
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

#[derive(Debug, Clone)]
pub struct DbDiagnostics {
    pub wal_bytes: u64,
    pub total_table_segment_bytes: u64,
    pub wal_ratio: Option<f64>,
    pub pending_rows: u64,
    pub pending_tables: u64,
    pub writes_per_sec: u64,
    pub last_write_age_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ColumnDiagnostics {
    pub column: String,
    pub col_type: ColumnType,
    pub logical_bytes: u64,
    pub stored_bytes: u64,
    pub compression_ratio: Option<f64>,
    pub rows: u64,
    pub nulls: u64,
}

#[derive(Debug, Clone)]
pub struct TableDiagnostics {
    pub table: String,
    pub rows: u64,
    pub segments: u64,
    pub ts_compression_ratio: Option<f64>,
    pub value_compression_ratio: Option<f64>,
    pub total_compression_ratio: Option<f64>,
    pub columns: Vec<ColumnDiagnostics>,
}

#[derive(Debug, Clone)]
pub struct MaintenanceDiagnostics {
    pub pack_success: u64,
    pub pack_fail: u64,
    pub retention_success: u64,
    pub retention_fail: u64,
    pub last_pack_error: Option<String>,
    pub last_retention_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DbDiagnosticsReport {
    pub wal_bytes: u64,
    pub total_table_segment_bytes: u64,
    pub wal_ratio: Option<f64>,
    pub pending_rows: u64,
    pub pending_tables: u64,
    pub writes_per_sec: u64,
    pub last_write_age_ms: u64,
    pub tables: Vec<TableDiagnostics>,
    pub maintenance: MaintenanceDiagnostics,
}

#[derive(Debug, Clone)]
struct MappedPredicate {
    col_idx: usize,
    op: ColumnPredicateOp,
    value: ColumnPredicateValue,
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

fn map_predicates_for_schema(
    schema: &TableSchema,
    predicates: &[ColumnPredicate],
) -> Vec<MappedPredicate> {
    let mut out = Vec::new();
    for pred in predicates {
        let idx = match schema
            .columns
            .iter()
            .position(|c| c.name == pred.column)
        {
            Some(idx) => idx,
            None => continue,
        };
        let col_type = schema.columns[idx].col_type;
        let matches = match (&pred.value, col_type) {
            (ColumnPredicateValue::F64(_), ColumnType::F64) => true,
            (ColumnPredicateValue::I64(_), ColumnType::I64) => true,
            (ColumnPredicateValue::Bool(_), ColumnType::Bool) => true,
            (ColumnPredicateValue::Utf8(_), ColumnType::Utf8) => true,
            _ => false,
        };
        if !matches {
            continue;
        }
        out.push(MappedPredicate {
            col_idx: idx,
            op: pred.op,
            value: pred.value.clone(),
        });
    }
    out
}

fn row_matches_predicates(values: &[Value], preds: &[MappedPredicate]) -> bool {
    for pred in preds {
        let ok = match (values.get(pred.col_idx), &pred.value) {
            (Some(Value::F64(v)), ColumnPredicateValue::F64(p)) => match pred.op {
                ColumnPredicateOp::Eq => v == p,
                ColumnPredicateOp::Gt => v > p,
                ColumnPredicateOp::GtEq => v >= p,
                ColumnPredicateOp::Lt => v < p,
                ColumnPredicateOp::LtEq => v <= p,
            },
            (Some(Value::I64(v)), ColumnPredicateValue::I64(p)) => match pred.op {
                ColumnPredicateOp::Eq => v == p,
                ColumnPredicateOp::Gt => v > p,
                ColumnPredicateOp::GtEq => v >= p,
                ColumnPredicateOp::Lt => v < p,
                ColumnPredicateOp::LtEq => v <= p,
            },
            (Some(Value::Bool(v)), ColumnPredicateValue::Bool(p)) => match pred.op {
                ColumnPredicateOp::Eq => v == p,
                ColumnPredicateOp::Gt => (*v as u8) > (*p as u8),
                ColumnPredicateOp::GtEq => (*v as u8) >= (*p as u8),
                ColumnPredicateOp::Lt => (*v as u8) < (*p as u8),
                ColumnPredicateOp::LtEq => (*v as u8) <= (*p as u8),
            },
            (Some(Value::Utf8(v)), ColumnPredicateValue::Utf8(p)) => match pred.op {
                ColumnPredicateOp::Eq => v == p,
                ColumnPredicateOp::Gt => v > p,
                ColumnPredicateOp::GtEq => v >= p,
                ColumnPredicateOp::Lt => v < p,
                ColumnPredicateOp::LtEq => v <= p,
            },
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
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

fn parse_sql_column_type(ty: &str) -> Option<ColumnType> {
    match ty.trim().to_ascii_lowercase().as_str() {
        "f64" | "float" | "double" | "real" => Some(ColumnType::F64),
        "i64" | "int" | "integer" | "bigint" => Some(ColumnType::I64),
        "bool" | "boolean" => Some(ColumnType::Bool),
        "text" | "string" | "utf8" => Some(ColumnType::Utf8),
        _ => None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn coerce_value_for_type(value: Value, col_type: ColumnType) -> io::Result<Value> {
    match (value, col_type) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::F64(v), ColumnType::F64) => Ok(Value::F64(v)),
        (Value::I64(v), ColumnType::I64) => Ok(Value::I64(v)),
        (Value::Bool(v), ColumnType::Bool) => Ok(Value::Bool(v)),
        (Value::Utf8(v), ColumnType::Utf8) => Ok(Value::Utf8(v)),
        (Value::I64(v), ColumnType::F64) => Ok(Value::F64(v as f64)),
        (Value::F64(v), ColumnType::I64) => {
            if v.is_finite() && v.fract() == 0.0 {
                Ok(Value::I64(v as i64))
            } else {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "type mismatch"))
            }
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "type mismatch")),
    }
}

fn parse_sql_values(values: &str) -> io::Result<Vec<Value>> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut chars = values.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if in_string {
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    buf.push('\'');
                    chars.next();
                } else {
                    in_string = false;
                }
            } else {
                buf.push(ch);
            }
            continue;
        }
        match ch {
            '\'' => {
                in_string = true;
            }
            ',' => {
                out.push(parse_sql_value_token(&buf)?);
                buf.clear();
            }
            _ => {
                buf.push(ch);
            }
        }
    }
    if in_string {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "unterminated string"));
    }
    if !buf.trim().is_empty() || values.trim().ends_with(',') {
        out.push(parse_sql_value_token(&buf)?);
    }
    Ok(out)
}

fn parse_sql_value_rows(values: &str) -> io::Result<Vec<Vec<Value>>> {
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut buf = String::new();
    let mut chars = values.chars().peekable();
    let mut in_string = false;
    let mut depth = 0usize;

    while let Some(ch) = chars.next() {
        if in_string {
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    buf.push('\'');
                    chars.next();
                } else {
                    in_string = false;
                }
            } else {
                buf.push(ch);
            }
            continue;
        }
        match ch {
            '\'' => in_string = true,
            '(' => {
                if depth == 0 {
                    buf.clear();
                } else {
                    buf.push(ch);
                }
                depth += 1;
            }
            ')' => {
                if depth == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unmatched closing paren",
                    ));
                }
                depth -= 1;
                if depth == 0 {
                    rows.push(parse_sql_values(&buf)?);
                    buf.clear();
                } else {
                    buf.push(ch);
                }
            }
            ',' => {
                if depth > 0 {
                    buf.push(ch);
                }
            }
            _ => {
                if depth > 0 {
                    buf.push(ch);
                } else if !ch.is_whitespace() && ch != ';' {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unexpected token outside values list",
                    ));
                }
            }
        }
    }
    if in_string {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "unterminated string"));
    }
    if depth != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unterminated values list",
        ));
    }
    if rows.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing VALUES rows",
        ));
    }
    Ok(rows)
}

fn parse_sql_value_token(token: &str) -> io::Result<Value> {
    let t = token.trim();
    if t.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty value"));
    }
    let lower = t.to_ascii_lowercase();
    if lower == "null" {
        return Ok(Value::Null);
    }
    if lower == "true" {
        return Ok(Value::Bool(true));
    }
    if lower == "false" {
        return Ok(Value::Bool(false));
    }
    if let Ok(v) = t.parse::<i64>() {
        return Ok(Value::I64(v));
    }
    if let Ok(v) = t.parse::<f64>() {
        return Ok(Value::F64(v));
    }
    Ok(Value::Utf8(t.to_string()))
}

fn parse_sql_ident(ident: &str) -> io::Result<String> {
    let ident = ident.trim();
    if ident.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty identifier"));
    }
    let bytes = ident.as_bytes();
    if (bytes.first() == Some(&b'"') && bytes.last() == Some(&b'"'))
        || (bytes.first() == Some(&b'`') && bytes.last() == Some(&b'`'))
    {
        let quote = bytes[0] as char;
        let inner = &ident[1..ident.len() - 1];
        let mut out = String::new();
        let mut chars = inner.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == quote {
                if chars.peek() == Some(&quote) {
                    out.push(quote);
                    chars.next();
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid identifier",
                    ));
                }
            } else {
                out.push(ch);
            }
        }
        if out.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty identifier"));
        }
        return Ok(out);
    }
    if !ident
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid identifier",
        ));
    }
    Ok(ident.to_string())
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
    wal_bytes: u64,
    options: NanoTsOptions,
    tables: HashMap<String, TableBuffer>,
    write_stats: WriteStats,
    maintenance_stats: MaintenanceStats,
}

#[derive(Debug)]
struct WriteStats {
    window_start: Instant,
    writes: u64,
    last_write: Instant,
}

#[derive(Debug, Default, Clone)]
struct MaintenanceStats {
    pack_success: u64,
    pack_fail: u64,
    retention_success: u64,
    retention_fail: u64,
    last_pack_error: Option<String>,
    last_retention_error: Option<String>,
}

#[derive(Clone)]
pub struct NanoTsDbShared {
    inner: Arc<RwLock<NanoTsDb>>,
}

impl NanoTsDbShared {
    pub fn open(root: impl AsRef<Path>, options: NanoTsOptions) -> io::Result<Self> {
        let auto_opts = options.auto_maintenance.clone();
        let db = NanoTsDb::open(root, options)?;
        let shared = Self {
            inner: Arc::new(RwLock::new(db)),
        };
        if let Some(opts) = auto_opts {
            spawn_auto_maintenance(Arc::downgrade(&shared.inner), opts);
        }
        Ok(shared)
    }

    pub fn with_read<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&NanoTsDb) -> io::Result<R>,
    {
        let guard = self.inner.read().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "db read lock poisoned")
        })?;
        f(&guard)
    }

    pub fn with_write<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut NanoTsDb) -> io::Result<R>,
    {
        let mut guard = self.inner.write().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "db write lock poisoned")
        })?;
        f(&mut guard)
    }
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
        let wal_bytes = wal.bytes_since_checkpoint()?;

        let mut db = Self {
            storage,
            wal,
            meta,
            next_seq: 1,
            wal_bytes,
            options,
            tables: HashMap::new(),
            write_stats: WriteStats {
                window_start: Instant::now(),
                writes: 0,
                last_write: Instant::now(),
            },
            maintenance_stats: MaintenanceStats::default(),
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

    fn record_write(&mut self) {
        let Some(opts) = &self.options.auto_maintenance else {
            return;
        };
        let elapsed = self.write_stats.window_start.elapsed();
        if elapsed >= opts.write_load_window {
            self.write_stats.window_start = Instant::now();
            self.write_stats.writes = 0;
        }
        self.write_stats.writes = self.write_stats.writes.saturating_add(1);
        self.write_stats.last_write = Instant::now();
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

    pub fn append_table_batch(
        &mut self,
        table: &str,
        ts_ms: &[i64],
        cols: &[ColumnData],
    ) -> io::Result<u64> {
        if ts_ms.is_empty() {
            return Ok(0);
        }
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
        let schema = self.tables.get(table).unwrap().schema.clone();
        if cols.len() != schema.columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "column length mismatch",
            ));
        }
        for (col, schema_col) in cols.iter().zip(schema.columns.iter()) {
            let len = match col {
                ColumnData::F64(v) => v.len(),
                ColumnData::I64(v) => v.len(),
                ColumnData::Bool(v) => v.len(),
                ColumnData::Utf8(v) => v.len(),
            };
            if len != ts_ms.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "column length mismatch",
                ));
            }
            let type_ok = matches!(
                (col, schema_col.col_type),
                (ColumnData::F64(_), ColumnType::F64)
                    | (ColumnData::I64(_), ColumnType::I64)
                    | (ColumnData::Bool(_), ColumnType::Bool)
                    | (ColumnData::Utf8(_), ColumnType::Utf8)
            );
            if !type_ok {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "column type mismatch",
                ));
            }
        }

        let mut values: Vec<Value> = vec![Value::Null; schema.columns.len()];
        let mut appended = 0u64;
        for row in 0..ts_ms.len() {
            for (cidx, col) in cols.iter().enumerate() {
                values[cidx] = match col {
                    ColumnData::F64(v) => v[row].map(Value::F64).unwrap_or(Value::Null),
                    ColumnData::I64(v) => v[row].map(Value::I64).unwrap_or(Value::Null),
                    ColumnData::Bool(v) => v[row].map(Value::Bool).unwrap_or(Value::Null),
                    ColumnData::Utf8(v) => v[row]
                        .as_ref()
                        .map(|s| Value::Utf8(s.clone()))
                        .unwrap_or(Value::Null),
                };
            }
            self.append_row_values(table, ts_ms[row], &values)?;
            appended = appended.saturating_add(1);
        }
        Ok(appended)
    }

    #[cfg(feature = "arrow")]
    pub fn append_table_batch_arrow(
        &mut self,
        table: &str,
        schema: *mut crate::arrow::ArrowSchema,
        array: *mut crate::arrow::ArrowArray,
    ) -> io::Result<u64> {
        let batch = crate::arrow::import_ts_table_from_c(schema, array).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("arrow import failed: {}", e))
        })?;
        let schema = self.table_schema(table)?;
        let mut column_map = batch.columns;
        let mut cols: Vec<ColumnData> = Vec::with_capacity(schema.columns.len());
        for col in &schema.columns {
            let data = column_map
                .remove(&col.name)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing column {}", col.name),
                    )
                })?;
            cols.push(data);
        }
        self.append_table_batch(table, &batch.ts_ms, &cols)
    }

    fn append_row_values(
        &mut self,
        table: &str,
        ts_ms: i64,
        values: &[Value],
    ) -> io::Result<u64> {
        let schema = self.tables.get(table).unwrap().schema.clone();
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

        self.record_write();
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);

        let all_f64 = schema.columns.iter().all(|c| c.col_type == ColumnType::F64);
        let has_null = values.iter().any(|v| matches!(v, Value::Null));
        if all_f64 && !has_null {
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
            let wal_bytes = self.wal.append_with_size(WalRecord::AppendRow {
                seq,
                table,
                ts_ms,
                values: &raw,
            })?;
            self.wal_bytes = self.wal_bytes.saturating_add(wal_bytes);
        } else {
            let wal_values = values.iter().map(to_wal_value).collect::<Vec<_>>();
            let wal_bytes = self.wal.append_with_size(WalRecord::AppendRowTyped {
                seq,
                table,
                ts_ms,
                values: &wal_values,
            })?;
            self.wal_bytes = self.wal_bytes.saturating_add(wal_bytes);
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
        self.storage.write_meta(self.meta.last_seq, self.meta.retention_ms)?;
        for t in self.tables.keys() {
            self.storage.write_table_index(t)?;
        }
        self.storage.write_footer()?;
        self.wal.reset()?;
        self.wal_bytes = 0;
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
        self.query_table_range_typed_with_predicates(table, start_ms, end_ms, &[])
    }

    pub fn query_table_range_typed_with_predicates(
        &self,
        table: &str,
        start_ms: i64,
        end_ms: i64,
        predicates: &[ColumnPredicate],
    ) -> io::Result<(Vec<i64>, Vec<ColumnData>)> {
        let effective_start = match self.retention_cutoff_ms() {
            Some(cutoff) => start_ms.max(cutoff),
            None => start_ms,
        };
        let schema = self.storage.read_table_schema(table)?;
        let mapped_predicates = map_predicates_for_schema(&schema, predicates);
        let mut out = self.storage.read_table_columns_in_range_filtered(
            table,
            &schema,
            effective_start,
            end_ms,
            predicates,
        )?;

        if let Some(buf) = self.tables.get(table) {
            for r in &buf.rows {
                if r.ts_ms < effective_start || r.ts_ms > end_ms {
                    continue;
                }
                if !mapped_predicates.is_empty() && !row_matches_predicates(&r.values, &mapped_predicates)
                {
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

    pub fn query_table_range_typed_snapshot_with_predicates(
        &self,
        table: &str,
        start_ms: i64,
        end_ms: i64,
        predicates: &[ColumnPredicate],
    ) -> io::Result<(Vec<i64>, Vec<ColumnData>)> {
        let effective_start = match self.retention_cutoff_ms() {
            Some(cutoff) => start_ms.max(cutoff),
            None => start_ms,
        };
        let schema = self.storage.read_table_schema(table)?;
        let snapshot_end = self.storage.file_len()?;
        self.storage.read_table_columns_in_range_filtered_with_limit(
            table,
            &schema,
            effective_start,
            end_ms,
            predicates,
            Some(snapshot_end),
        )
    }

    fn retention_cutoff_ms(&self) -> Option<i64> {
        let retention_ms = self.meta.retention_ms?;
        Some(now_ms().saturating_sub(retention_ms))
    }

    fn execute_create_table(&mut self, sql: &str) -> io::Result<()> {
        let lower = sql.to_ascii_lowercase();
        let mut rest = &sql["create table".len()..];
        if lower["create table".len()..].trim_start().starts_with("if not exists") {
            let idx = lower.find("if not exists").unwrap();
            rest = &sql[idx + "if not exists".len()..];
        }
        rest = rest.trim_start();
        let open = rest.find('(').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing column list")
        })?;
        let close = rest.rfind(')').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing closing paren")
        })?;
        if close <= open {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad column list"));
        }
        let name = parse_sql_ident(rest[..open].trim())?;
        let cols_str = &rest[open + 1..close];
        let mut cols: Vec<(&str, ColumnType)> = Vec::new();
        for col_def in cols_str.split(',') {
            let mut parts = col_def.split_whitespace();
            let col_name = parts
                .next()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bad column"))?;
            let col_type = parts
                .next()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bad column type"))?;
            let col_type = parse_sql_column_type(col_type).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "unsupported column type")
            })?;
            cols.push((col_name, col_type));
        }
        self.create_table_typed(&name, &cols)
    }

    fn execute_insert(&mut self, sql: &str) -> io::Result<()> {
        let lower = sql.to_ascii_lowercase();
        let values_idx = lower
            .find("values")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing VALUES"))?;
        let table_part = sql["insert into".len()..values_idx].trim();
        let (table, column_list) = if let Some(open) = table_part.find('(') {
            let close = table_part.rfind(')').ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing closing paren")
            })?;
            if close <= open {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "bad column list",
                ));
            }
            let table = parse_sql_ident(table_part[..open].trim())?;
            let cols_raw = &table_part[open + 1..close];
            let cols = cols_raw
                .split(',')
                .map(|c| parse_sql_ident(c))
                .collect::<io::Result<Vec<String>>>()?;
            (table, Some(cols))
        } else {
            (parse_sql_ident(table_part)?, None)
        };

        let values_part = &sql[values_idx + "values".len()..];
        let rows = parse_sql_value_rows(values_part)?;
        let schema = self.storage.read_table_schema(&table)?;

        for row in rows {
            if let Some(cols) = &column_list {
                if row.len() != cols.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "values length mismatch",
                    ));
                }
                let mut values = vec![Value::Null; schema.columns.len()];
                let mut ts_ms: Option<i64> = None;
                for (name, value) in cols.iter().zip(row.into_iter()) {
                    if name == "ts_ms" || name == "ts" {
                        let ts = match value {
                            Value::I64(v) => v,
                            Value::F64(v) if v.is_finite() && v.fract() == 0.0 => v as i64,
                            _ => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "invalid ts_ms value",
                                ))
                            }
                        };
                        ts_ms = Some(ts);
                        continue;
                    }
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == *name)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "unknown column")
                        })?;
                    let coerced = coerce_value_for_type(value, schema.columns[idx].col_type)?;
                    values[idx] = coerced;
                }
                let ts_ms = ts_ms.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing ts_ms")
                })?;
                self.append_row_typed(&table, ts_ms, &values)?;
            } else {
                if row.len() != schema.columns.len() + 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "values length mismatch",
                    ));
                }
                let mut values = row;
                let ts_value = values.remove(0);
                let ts_ms = match ts_value {
                    Value::I64(v) => v,
                    Value::F64(v) if v.is_finite() && v.fract() == 0.0 => v as i64,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid ts_ms value",
                        ))
                    }
                };
                for (value, col) in values.iter_mut().zip(schema.columns.iter()) {
                    let coerced = coerce_value_for_type(value.clone(), col.col_type)?;
                    *value = coerced;
                }
                self.append_row_typed(&table, ts_ms, &values)?;
            }
        }
        Ok(())
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

    pub fn diagnose(&self) -> io::Result<DbDiagnostics> {
        let total_table_segment_bytes = self.storage.total_table_segment_bytes()?;
        let wal_ratio = if total_table_segment_bytes > 0 {
            Some(self.wal_bytes as f64 / total_table_segment_bytes as f64)
        } else {
            None
        };
        let pending_rows = self
            .tables
            .values()
            .map(|buf| buf.rows.len() as u64)
            .sum::<u64>();
        let pending_tables = self
            .tables
            .values()
            .filter(|buf| !buf.rows.is_empty())
            .count() as u64;
        let elapsed = self.write_stats.window_start.elapsed();
        let writes_per_sec = if elapsed.as_millis() == 0 {
            0
        } else {
            (self.write_stats.writes as u128 * 1000 / elapsed.as_millis())
                .min(u64::MAX as u128) as u64
        };
        let last_write_age_ms = self
            .write_stats
            .last_write
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        Ok(DbDiagnostics {
            wal_bytes: self.wal_bytes,
            total_table_segment_bytes,
            wal_ratio,
            pending_rows,
            pending_tables,
            writes_per_sec,
            last_write_age_ms,
        })
    }

    pub fn get_diagnostics(&self) -> io::Result<DbDiagnosticsReport> {
        let base = self.diagnose()?;
        let mut tables = Vec::new();
        for table in self.storage.list_tables()? {
            let schema = self.storage.read_table_schema(&table)?;
            let table_stats = self.storage.table_stats(&table)?;
            let col_stats = self.storage.table_column_stats(&table)?;
            let mut columns = Vec::with_capacity(schema.columns.len());
            let mut logical_value_bytes = 0u64;
            let mut stored_value_bytes = 0u64;
            for (col, stats) in schema.columns.iter().zip(col_stats.iter()) {
                logical_value_bytes = logical_value_bytes.saturating_add(stats.logical_bytes);
                stored_value_bytes = stored_value_bytes.saturating_add(stats.stored_bytes);
                let compression_ratio = if stats.stored_bytes > 0 {
                    Some(stats.logical_bytes as f64 / stats.stored_bytes as f64)
                } else {
                    None
                };
                columns.push(ColumnDiagnostics {
                    column: col.name.clone(),
                    col_type: col.col_type,
                    logical_bytes: stats.logical_bytes,
                    stored_bytes: stats.stored_bytes,
                    compression_ratio,
                    rows: stats.rows,
                    nulls: stats.nulls,
                });
            }
            let ts_compression_ratio = if table_stats.stored_ts_bytes > 0 {
                Some(table_stats.raw_ts_bytes as f64 / table_stats.stored_ts_bytes as f64)
            } else {
                None
            };
            let value_compression_ratio = if stored_value_bytes > 0 {
                Some(logical_value_bytes as f64 / stored_value_bytes as f64)
            } else {
                None
            };
            let total_stored = table_stats
                .stored_ts_bytes
                .saturating_add(stored_value_bytes);
            let total_logical = table_stats
                .raw_ts_bytes
                .saturating_add(logical_value_bytes);
            let total_compression_ratio = if total_stored > 0 {
                Some(total_logical as f64 / total_stored as f64)
            } else {
                None
            };
            tables.push(TableDiagnostics {
                table,
                rows: table_stats.rows,
                segments: table_stats.segments,
                ts_compression_ratio,
                value_compression_ratio,
                total_compression_ratio,
                columns,
            });
        }

        Ok(DbDiagnosticsReport {
            wal_bytes: base.wal_bytes,
            total_table_segment_bytes: base.total_table_segment_bytes,
            wal_ratio: base.wal_ratio,
            pending_rows: base.pending_rows,
            pending_tables: base.pending_tables,
            writes_per_sec: base.writes_per_sec,
            last_write_age_ms: base.last_write_age_ms,
            tables,
            maintenance: self.maintenance_snapshot(),
        })
    }

    pub fn last_seq(&self) -> u64 {
        self.meta.last_seq
    }

    pub fn execute_sql(&mut self, sql: &str) -> io::Result<()> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Ok(());
        }
        let lower = sql.to_ascii_lowercase();
        if lower.starts_with("create table") {
            return self.execute_create_table(sql);
        }
        if lower.starts_with("insert into") {
            return self.execute_insert(sql);
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported SQL statement",
        ))
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
        let cutoff = now_ms().saturating_sub(retention_ms);
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

    fn pack_all_tables(&mut self, target_segment_points: usize) -> io::Result<()> {
        self.flush()?;
        for table in self.storage.list_tables()? {
            self.storage.pack_table(&table, target_segment_points)?;
        }
        Ok(())
    }

    fn pack_all_tables_tiered(
        &mut self,
        target_segment_points: usize,
        max_segments_per_pack: usize,
    ) -> io::Result<()> {
        self.flush()?;
        for table in self.storage.list_tables()? {
            self.storage
                .pack_table_tiered(&table, target_segment_points, max_segments_per_pack)?;
        }
        Ok(())
    }

    fn should_auto_pack(&self, opts: &AutoMaintenanceOptions) -> io::Result<bool> {
        if self.wal_bytes < opts.wal_min_bytes {
            return Ok(false);
        }
        let total_bytes = self.storage.total_table_segment_bytes()?;
        if total_bytes == 0 {
            return Ok(false);
        }
        Ok(self.wal_bytes >= total_bytes.saturating_mul(opts.wal_target_ratio))
    }

    fn write_load_high(&self, opts: &AutoMaintenanceOptions) -> bool {
        if opts.max_writes_per_sec == 0 {
            return false;
        }
        let elapsed = self.write_stats.window_start.elapsed();
        if elapsed.as_millis() == 0 {
            return true;
        }
        let writes_per_sec = (self.write_stats.writes as u128 * 1000 / elapsed.as_millis())
            .min(u64::MAX as u128) as u64;
        writes_per_sec >= opts.max_writes_per_sec
    }

    fn idle_for_pack(&self, opts: &AutoMaintenanceOptions) -> bool {
        self.write_stats.last_write.elapsed() >= opts.min_idle_time
    }

    fn record_pack_result(&mut self, result: &io::Result<()>) {
        match result {
            Ok(()) => {
                self.maintenance_stats.pack_success =
                    self.maintenance_stats.pack_success.saturating_add(1);
                self.maintenance_stats.last_pack_error = None;
            }
            Err(err) => {
                self.maintenance_stats.pack_fail =
                    self.maintenance_stats.pack_fail.saturating_add(1);
                self.maintenance_stats.last_pack_error = Some(err.to_string());
            }
        }
    }

    fn record_retention_result(&mut self, result: &io::Result<()>) {
        match result {
            Ok(()) => {
                self.maintenance_stats.retention_success =
                    self.maintenance_stats.retention_success.saturating_add(1);
                self.maintenance_stats.last_retention_error = None;
            }
            Err(err) => {
                self.maintenance_stats.retention_fail =
                    self.maintenance_stats.retention_fail.saturating_add(1);
                self.maintenance_stats.last_retention_error = Some(err.to_string());
            }
        }
    }

    fn maintenance_snapshot(&self) -> MaintenanceDiagnostics {
        MaintenanceDiagnostics {
            pack_success: self.maintenance_stats.pack_success,
            pack_fail: self.maintenance_stats.pack_fail,
            retention_success: self.maintenance_stats.retention_success,
            retention_fail: self.maintenance_stats.retention_fail,
            last_pack_error: self.maintenance_stats.last_pack_error.clone(),
            last_retention_error: self.maintenance_stats.last_retention_error.clone(),
        }
    }
}

fn spawn_auto_maintenance(db: Weak<RwLock<NanoTsDb>>, opts: AutoMaintenanceOptions) {
    std::thread::spawn(move || {
        let mut last_retention = std::time::Instant::now()
            .checked_sub(opts.retention_check_interval)
            .unwrap_or_else(std::time::Instant::now);
        loop {
            std::thread::sleep(opts.check_interval);
            let Some(db) = db.upgrade() else { break };
            let mut guard = match db.write() {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            if guard.options.retention.is_some()
                && last_retention.elapsed() >= opts.retention_check_interval
            {
                if !guard.write_load_high(&opts) && guard.idle_for_pack(&opts) {
                    let result = guard.compact_retention_now();
                    guard.record_retention_result(&result);
                    last_retention = std::time::Instant::now();
                }
            }
            let should_pack = match guard.should_auto_pack(&opts) {
                Ok(should_pack) => should_pack,
                Err(_) => continue,
            };
            if should_pack && !guard.write_load_high(&opts) && guard.idle_for_pack(&opts) {
                let result =
                    guard.pack_all_tables_tiered(opts.target_segment_points, opts.max_segments_per_pack);
                guard.record_pack_result(&result);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbfile;
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};
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
            let base = now_ms().saturating_sub(2_000);
            for i in 0..1000i64 {
                db.append_row(
                    "sensor",
                    base + i,
                    &[25.0 + (i as f64) * 0.01, 60.0],
                )
                    .unwrap();
            }
            db.flush().unwrap();
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let base = now_ms().saturating_sub(2_000);
            let (ts, cols) =
                db.query_table_range_columns("sensor", base + 200, base + 300).unwrap();
            assert!(!ts.is_empty());
            assert_eq!(cols.len(), 2);
            assert_eq!(ts.len(), cols[0].len());
            assert_eq!(ts.len(), cols[1].len());
            assert!(ts[0] >= base + 200 && *ts.last().unwrap() <= base + 300);
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
    fn test_get_diagnostics_report() {
        let path = fresh_db_path("diagnostics_report");
        let mut db = NanoTsDb::open(&path, NanoTsOptions::default()).unwrap();
        db.create_table_typed(
            "metrics",
            &[
                ("temp", ColumnType::F64),
                ("flag", ColumnType::Bool),
                ("tag", ColumnType::Utf8),
            ],
        )
        .unwrap();
        db.append_row_typed(
            "metrics",
            1000,
            &[
                Value::F64(1.0),
                Value::Bool(true),
                Value::Utf8("a".to_string()),
            ],
        )
        .unwrap();
        db.append_row_typed(
            "metrics",
            1001,
            &[
                Value::F64(2.0),
                Value::Bool(false),
                Value::Utf8("a".to_string()),
            ],
        )
        .unwrap();
        db.flush().unwrap();

        let diag = db.get_diagnostics().unwrap();
        assert_eq!(diag.tables.len(), 1);
        let table = &diag.tables[0];
        assert_eq!(table.table, "metrics");
        assert_eq!(table.columns.len(), 3);
        assert!(table.value_compression_ratio.is_some());
        for col in &table.columns {
            assert!(col.rows >= 2);
            assert!(col.stored_bytes > 0);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_append_table_batch() {
        let path = fresh_db_path("append_table_batch");
        let mut db = NanoTsDb::open(&path, NanoTsOptions::default()).unwrap();
        db.create_table_typed(
            "t",
            &[
                ("temp", ColumnType::F64),
                ("flag", ColumnType::Bool),
                ("tag", ColumnType::Utf8),
            ],
        )
        .unwrap();

        let ts = vec![1000, 1001, 1002];
        let cols = vec![
            ColumnData::F64(vec![Some(1.0), Some(2.0), None]),
            ColumnData::Bool(vec![Some(true), None, Some(false)]),
            ColumnData::Utf8(vec![
                Some("a".to_string()),
                None,
                Some("b".to_string()),
            ]),
        ];
        let appended = db.append_table_batch("t", &ts, &cols).unwrap();
        assert_eq!(appended, 3);
        db.flush().unwrap();

        let (out_ts, out_cols) = db.query_table_range_typed("t", 0, 10_000).unwrap();
        assert_eq!(out_ts.len(), 3);
        assert_eq!(out_cols.len(), 3);
        assert_eq!(out_ts, ts);
        match &out_cols[0] {
            ColumnData::F64(v) => assert_eq!(v[0], Some(1.0)),
            _ => panic!("unexpected column type"),
        }
        let _ = fs::remove_file(path);
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn test_append_table_batch_arrow() {
        let path = fresh_db_path("append_table_batch_arrow");
        let mut db = NanoTsDb::open(&path, NanoTsOptions::default()).unwrap();
        db.create_table_typed(
            "t",
            &[
                ("temp", ColumnType::F64),
                ("flag", ColumnType::Bool),
                ("tag", ColumnType::Utf8),
            ],
        )
        .unwrap();

        let ts = vec![1000, 1001, 1002];
        let cols = vec![
            ("temp", ColumnData::F64(vec![Some(1.0), Some(2.0), None])),
            ("flag", ColumnData::Bool(vec![Some(true), None, Some(false)])),
            (
                "tag",
                ColumnData::Utf8(vec![
                    Some("a".to_string()),
                    None,
                    Some("b".to_string()),
                ]),
            ),
        ];
        let mut batch = crate::arrow::ArrowBatch::from_ts_columns("t", ts.clone(), cols).unwrap();
        let appended = db
            .append_table_batch_arrow("t", batch.schema_ptr(), batch.array_ptr())
            .unwrap();
        assert_eq!(appended, 3);
        db.flush().unwrap();

        let (out_ts, out_cols) = db.query_table_range_typed("t", 0, 10_000).unwrap();
        assert_eq!(out_ts, ts);
        match &out_cols[0] {
            ColumnData::F64(v) => assert_eq!(v[1], Some(2.0)),
            _ => panic!("unexpected column type"),
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

    #[test]
    fn test_table_checksum_enforced_on_read() {
        let path = fresh_db_path("checksum_read");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table("t", &["v"]).unwrap();
            for i in 0..10i64 {
                db.append_row("t", 1000 + i, &[i as f64]).unwrap();
            }
            db.flush().unwrap();
        }
        let db = NanoTsDb::open(&path, opts).unwrap();

        let mut checksum_offset: Option<u64> = None;
        dbfile::iter_records(&path, |hdr, _| {
            if hdr.record_type == dbfile::RECORD_TABLE_SEGMENT && checksum_offset.is_none() {
                checksum_offset = Some(hdr.payload_offset + hdr.payload_len as u64);
            }
            Ok(())
        })
        .unwrap();
        let checksum_offset = checksum_offset.expect("table segment checksum offset");

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        let mut b = [0u8; 1];
        file.read_exact(&mut b).unwrap();
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        b[0] ^= 0xff;
        file.write_all(&b).unwrap();
        file.flush().unwrap();

        let err = db
            .query_table_range_columns("t", 0, 10_000)
            .expect_err("checksum corruption should fail read");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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

    #[test]
    fn test_execute_sql_create_insert() {
        let path = fresh_db_path("sql_exec");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.execute_sql("CREATE TABLE t (a i64, b bool, c utf8, d f64)")
                .unwrap();
            db.execute_sql("INSERT INTO t VALUES (1000, 1, true, 'x', 1.5)")
                .unwrap();
            db.execute_sql("INSERT INTO t VALUES (1001, null, false, 'y', 2)")
                .unwrap();
            db.execute_sql(
                "INSERT INTO t (ts_ms, c, d) VALUES (1002, 'z', 3.5), (1003, 'w', 4.5)",
            )
                .unwrap();
            db.flush().unwrap();
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let (ts, cols) = db.query_table_range_typed("t", 0, 10_000).unwrap();
            assert_eq!(ts.len(), 4);
            match &cols[0] {
                ColumnData::I64(values) => {
                    assert_eq!(values[0], Some(1));
                    assert_eq!(values[1], None);
                    assert_eq!(values[2], None);
                    assert_eq!(values[3], None);
                }
                _ => panic!("unexpected column type"),
            }
            match &cols[1] {
                ColumnData::Bool(values) => {
                    assert_eq!(values[0], Some(true));
                    assert_eq!(values[1], Some(false));
                    assert_eq!(values[2], None);
                    assert_eq!(values[3], None);
                }
                _ => panic!("unexpected column type"),
            }
            match &cols[2] {
                ColumnData::Utf8(values) => {
                    assert_eq!(values[0].as_deref(), Some("x"));
                    assert_eq!(values[1].as_deref(), Some("y"));
                    assert_eq!(values[2].as_deref(), Some("z"));
                    assert_eq!(values[3].as_deref(), Some("w"));
                }
                _ => panic!("unexpected column type"),
            }
            match &cols[3] {
                ColumnData::F64(values) => {
                    assert_eq!(values[0], Some(1.5));
                    assert_eq!(values[1], Some(2.0));
                    assert_eq!(values[2], Some(3.5));
                    assert_eq!(values[3], Some(4.5));
                }
                _ => panic!("unexpected column type"),
            }
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_wal_replay_typed_values() {
        let path = fresh_db_path("wal_typed");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table_typed(
                "t",
                &[
                    ("a", ColumnType::I64),
                    ("b", ColumnType::Bool),
                    ("c", ColumnType::Utf8),
                ],
            )
            .unwrap();
            db.append_row_typed(
                "t",
                1000,
                &[
                    Value::I64(7),
                    Value::Bool(true),
                    Value::Utf8("ok".to_string()),
                ],
            )
            .unwrap();
            // Drop without flush to force WAL replay.
        }
        {
            let db = NanoTsDb::open(&path, opts).unwrap();
            let (ts, cols) = db.query_table_range_typed("t", 0, 10_000).unwrap();
            assert_eq!(ts, vec![1000]);
            match &cols[0] {
                ColumnData::I64(values) => assert_eq!(values[0], Some(7)),
                _ => panic!("unexpected column type"),
            }
            match &cols[1] {
                ColumnData::Bool(values) => assert_eq!(values[0], Some(true)),
                _ => panic!("unexpected column type"),
            }
            match &cols[2] {
                ColumnData::Utf8(values) => assert_eq!(values[0].as_deref(), Some("ok")),
                _ => panic!("unexpected column type"),
            }
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_shared_read_write() {
        let path = fresh_db_path("shared_rw");
        let opts = NanoTsOptions::default();
        let db = NanoTsDbShared::open(&path, opts.clone()).unwrap();
        db.with_write(|db| db.create_table("t", &["v"]))
            .unwrap();

        let writer = {
            let db = db.clone();
            std::thread::spawn(move || {
                db.with_write(|db| {
                    for i in 0..100i64 {
                        db.append_row("t", 1_000 + i, &[i as f64])?;
                    }
                    db.flush()
                })
                .unwrap();
            })
        };

        let reader = {
            let db = db.clone();
            std::thread::spawn(move || {
                db.with_read(|db| {
                    let _ = db.query_table_range_columns("t", 0, 10_000)?;
                    Ok(())
                })
                .unwrap();
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();

        let db = NanoTsDb::open(&path, opts).unwrap();
        let (ts, cols) = db.query_table_range_columns("t", 0, 10_000).unwrap();
        assert_eq!(ts.len(), cols[0].len());
        assert!(ts.len() >= 100);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_query_predicates_filter_rows() {
        let path = fresh_db_path("predicates");
        let opts = NanoTsOptions::default();
        {
            let mut db = NanoTsDb::open(&path, opts.clone()).unwrap();
            db.create_table_typed(
                "t",
                &[
                    ("i", ColumnType::I64),
                    ("b", ColumnType::Bool),
                    ("s", ColumnType::Utf8),
                ],
            )
            .unwrap();
            db.append_row_typed(
                "t",
                1000,
                &[
                    Value::I64(1),
                    Value::Bool(true),
                    Value::Utf8("a".to_string()),
                ],
            )
            .unwrap();
            db.append_row_typed(
                "t",
                1001,
                &[
                    Value::I64(2),
                    Value::Bool(false),
                    Value::Utf8("b".to_string()),
                ],
            )
            .unwrap();
            db.flush().unwrap();
        }
        let db = NanoTsDb::open(&path, opts).unwrap();
        let preds = vec![ColumnPredicate {
            column: "b".to_string(),
            op: ColumnPredicateOp::Eq,
            value: ColumnPredicateValue::Bool(true),
        }];
        let (ts, cols) = db
            .query_table_range_typed_with_predicates("t", 0, 10_000, &preds)
            .unwrap();
        assert_eq!(ts, vec![1000]);
        match &cols[0] {
            ColumnData::I64(values) => assert_eq!(values[0], Some(1)),
            _ => panic!("unexpected column type"),
        }

        let preds = vec![ColumnPredicate {
            column: "s".to_string(),
            op: ColumnPredicateOp::Eq,
            value: ColumnPredicateValue::Utf8("b".to_string()),
        }];
        let (ts, cols) = db
            .query_table_range_typed_with_predicates("t", 0, 10_000, &preds)
            .unwrap();
        assert_eq!(ts, vec![1001]);
        match &cols[2] {
            ColumnData::Utf8(values) => assert_eq!(values[0].as_deref(), Some("b")),
            _ => panic!("unexpected column type"),
        }
        let _ = fs::remove_file(path);
    }
}
