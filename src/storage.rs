// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::compressor::TimeSeriesCompressor;
use crate::compressor::{compress_f64_xor, decompress_f64_xor};
use crate::db::{ColumnData, ColumnPredicate, ColumnPredicateOp, ColumnPredicateValue, ColumnSchema, ColumnType, Point, TableSchema};
use crate::dbfile;
use memmap2::MmapOptions;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const SEG_MAGIC: &[u8; 4] = b"NTSG";
const SEG_VERSION: u8 = 1;
const TS_CODEC_TS64: u8 = 1;

const TABLE_MAGIC: &[u8; 4] = b"NTTB";
const TABLE_VERSION: u8 = 1;
const TABLE_COL_F64_XOR: u8 = 2;
const TABLE_COL_I64_D2: u8 = 3;
const TABLE_COL_BOOL: u8 = 4;
const TABLE_COL_UTF8: u8 = 5;

const SCHEMA_MAGIC: &[u8; 4] = b"NTSC";
const SCHEMA_VERSION: u8 = 1;

const META_MAGIC: &[u8; 4] = b"NTSM";
const META_VERSION: u8 = 1;

const TABLE_INDEX_MAGIC: &[u8; 4] = b"NTSI";
const TABLE_INDEX_VERSION: u8 = 1;

const FOOTER_MAGIC: &[u8; 4] = b"NTSF";
const FOOTER_VERSION: u8 = 1;

#[derive(Debug, Clone)]
pub struct Storage {
    path: PathBuf,
    state: Arc<Mutex<StorageState>>,
}

#[derive(Debug, Default)]
struct StorageState {
    schemas: HashMap<String, TableSchema>,
    schema_bytes: HashMap<String, u64>,
    schema_offsets: HashMap<String, u64>,
    table_index: HashMap<String, Vec<TableIndexEntry>>,
    index_bytes: HashMap<String, u64>,
    index_offsets: HashMap<String, u64>,
    meta_offset: Option<u64>,
    max_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStats {
    pub rows: u64,
    pub segments: u64,
    pub columns: u16,

    pub raw_ts_bytes: u64,
    pub raw_value_bytes: u64,

    pub stored_ts_bytes: u64,
    pub stored_value_bytes: u64,

    pub header_bytes: u64,
    pub ntt_file_bytes: u64,
    pub idx_file_bytes: u64,
    pub schema_file_bytes: u64,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        dbfile::ensure_db_file(&path)?;
        let state = match load_state_from_footer(&path) {
            Ok(Some(state)) => state,
            Ok(None) => scan_db_file(&path)?,
            Err(_) => scan_db_file(&path)?,
        };
        Ok(Self {
            path,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn table_exists(&self, table: &str) -> bool {
        let state = self.state.lock().unwrap();
        state.schemas.contains_key(table)
    }

    pub fn list_series(&self) -> io::Result<Vec<String>> {
        let mut out: Vec<String> = Vec::new();
        dbfile::iter_records(&self.path, |hdr, payload| {
            if hdr.record_type != dbfile::RECORD_SERIES_SEGMENT {
                return Ok(());
            }
            let (name, _) = decode_name_prefix(payload)?;
            out.push(name);
            Ok(())
        })?;
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub fn list_tables(&self) -> io::Result<Vec<String>> {
        let state = self.state.lock().unwrap();
        let mut out: Vec<String> = state.schemas.keys().cloned().collect();
        out.sort();
        Ok(out)
    }

    pub fn table_segment_bytes(&self, table: &str) -> io::Result<u64> {
        let index = self.load_or_rebuild_table_index(table)?;
        Ok(index
            .iter()
            .fold(0u64, |acc, e| acc.saturating_add(e.len)))
    }

    pub fn total_table_segment_bytes(&self) -> io::Result<u64> {
        let mut total = 0u64;
        for table in self.list_tables()? {
            total = total.saturating_add(self.table_segment_bytes(&table)?);
        }
        Ok(total)
    }

    pub fn write_table_schema(&self, table: &str, schema: &TableSchema) -> io::Result<()> {
        let schema_bytes = encode_table_schema_bytes(schema)?;
        let payload = encode_named_payload(table, &schema_bytes)?;
        let record_offset = dbfile::append_record(&self.path, dbfile::RECORD_SCHEMA, &payload)?;

        let mut state = self.state.lock().unwrap();
        state.schemas.insert(table.to_string(), schema.clone());
        state.schema_bytes
            .insert(table.to_string(), schema_bytes.len() as u64);
        state
            .schema_offsets
            .insert(table.to_string(), record_offset);
        Ok(())
    }

    pub fn read_table_schema(&self, table: &str) -> io::Result<TableSchema> {
        {
            let state = self.state.lock().unwrap();
            if let Some(schema) = state.schemas.get(table) {
                return Ok(schema.clone());
            }
        }
        let new_state = scan_db_file(&self.path)?;
        let mut state = self.state.lock().unwrap();
        *state = new_state;
        state
            .schemas
            .get(table)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "table schema not found"))
    }

    pub fn read_latest_meta(&self) -> io::Result<Option<(u64, Option<i64>)>> {
        if let Some(offset) = self.state.lock().unwrap().meta_offset {
            if let Ok((hdr, payload)) = dbfile::read_record_at(&self.path, offset) {
                if hdr.record_type == dbfile::RECORD_META {
                    return Ok(Some(decode_meta_payload(&payload)?));
                }
            }
        }
        let mut out: Option<(u64, Option<i64>)> = None;
        dbfile::iter_records(&self.path, |hdr, payload| {
            if hdr.record_type != dbfile::RECORD_META {
                return Ok(());
            }
            let (last_seq, retention_ms) = decode_meta_payload(payload)?;
            out = Some((last_seq, retention_ms));
            Ok(())
        })?;
        Ok(out)
    }

    pub fn write_meta(&self, last_seq: u64, retention_ms: Option<i64>) -> io::Result<()> {
        let payload = encode_meta_payload(last_seq, retention_ms);
        let record_offset = dbfile::append_record(&self.path, dbfile::RECORD_META, &payload)?;
        let mut state = self.state.lock().unwrap();
        state.max_seq = state.max_seq.max(last_seq);
        state.meta_offset = Some(record_offset);
        Ok(())
    }

    pub fn write_footer(&self) -> io::Result<()> {
        let (meta_offset, schema_offsets, index_offsets) = {
            let state = self.state.lock().unwrap();
            (
                state.meta_offset,
                state.schema_offsets.clone(),
                state.index_offsets.clone(),
            )
        };
        if meta_offset.is_none() && schema_offsets.is_empty() && index_offsets.is_empty() {
            return Ok(());
        }
        let payload = encode_footer_payload(meta_offset, &schema_offsets, &index_offsets)?;
        let record_offset = dbfile::append_record(&self.path, dbfile::RECORD_FOOTER, &payload)?;
        dbfile::write_footer_offset(&self.path, record_offset)?;
        Ok(())
    }

    pub fn append_segment(&self, series: &str, points: &[Point], min_seq: u64, max_seq: u64) -> io::Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        let seg = encode_series_segment(points, min_seq, max_seq)?;
        let payload = encode_named_payload(series, &seg)?;
        let _ = dbfile::append_record(&self.path, dbfile::RECORD_SERIES_SEGMENT, &payload)?;
        {
            let mut state = self.state.lock().unwrap();
            state.max_seq = state.max_seq.max(min_seq).max(max_seq);
        }
        Ok(())
    }

    pub fn append_table_segment(
        &self,
        table: &str,
        schema: &TableSchema,
        ts_ms: &[i64],
        cols: &[ColumnData],
        min_seq: u64,
        max_seq: u64,
    ) -> io::Result<()> {
        if ts_ms.is_empty() {
            return Ok(());
        }
        if cols.len() != schema.columns.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "schema mismatch"));
        }
        for (c, col) in cols.iter().zip(schema.columns.iter()) {
            let len = match c {
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
            match (c, col.col_type) {
                (ColumnData::F64(_), ColumnType::F64)
                | (ColumnData::I64(_), ColumnType::I64)
                | (ColumnData::Bool(_), ColumnType::Bool)
                | (ColumnData::Utf8(_), ColumnType::Utf8) => {}
                _ => {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "schema mismatch"));
                }
            }
        }

        let segment_bytes = encode_table_segment(schema, ts_ms, cols, min_seq, max_seq)?;
        let payload = encode_named_payload(table, &segment_bytes)?;
        let record_offset =
            dbfile::append_record(&self.path, dbfile::RECORD_TABLE_SEGMENT, &payload)?;
        let name_len = table.as_bytes().len() as u64;
        let segment_offset = record_offset + 5 + 2 + name_len;
        let header = parse_table_segment_header(&segment_bytes)?;
        let col_stats = column_stats_from_cols(schema, cols);

        let mut state = self.state.lock().unwrap();
        state
            .table_index
            .entry(table.to_string())
            .or_default()
            .push(TableIndexEntry {
                offset: segment_offset,
                len: segment_bytes.len() as u64,
                min_ts: header.min_ts,
                max_ts: header.max_ts,
                min_seq: header.min_seq,
                max_seq: header.max_seq,
                count: header.count,
                col_stats,
            });
        state.max_seq = state.max_seq.max(min_seq).max(max_seq);
        Ok(())
    }

    pub fn write_table_index(&self, table: &str) -> io::Result<()> {
        let (entries, schema) = {
            let state = self.state.lock().unwrap();
            let entries = state.table_index.get(table).cloned().unwrap_or_default();
            let schema = state
                .schemas
                .get(table)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "table schema not found"))?;
            (entries, schema)
        };
        if entries.is_empty() {
            return Ok(());
        }
        let index_bytes = encode_table_index_payload(&entries, &schema)?;
        let payload = encode_named_payload(table, &index_bytes)?;
        let record_offset =
            dbfile::append_record(&self.path, dbfile::RECORD_TABLE_INDEX, &payload)?;
        let mut state = self.state.lock().unwrap();
        state
            .index_bytes
            .insert(table.to_string(), payload.len() as u64);
        state
            .index_offsets
            .insert(table.to_string(), record_offset);
        Ok(())
    }

    pub fn scan_max_seq(&self) -> io::Result<u64> {
        let state = self.state.lock().unwrap();
        Ok(state.max_seq)
    }

    pub fn read_points_in_range(&self, series: &str, start_ms: i64, end_ms: i64) -> io::Result<Vec<Point>> {
        let mut out = Vec::new();
        dbfile::iter_records(&self.path, |hdr, payload| {
            if hdr.record_type != dbfile::RECORD_SERIES_SEGMENT {
                return Ok(());
            }
            let (name, data) = decode_name_prefix(payload)?;
            if name != series {
                return Ok(());
            }
            let seg = read_series_segment_from_bytes(data)?;
            if seg.max_ts < start_ms || seg.min_ts > end_ms {
                return Ok(());
            }
            for i in 0..seg.count {
                let ts_ms = seg.ts_ms[i];
                if ts_ms < start_ms || ts_ms > end_ms {
                    continue;
                }
                out.push(Point {
                    ts_ms,
                    value: seg.values[i],
                });
            }
            Ok(())
        })?;

        Ok(out)
    }

    pub fn compact_retention(&self, series: &str, cutoff_ms: i64) -> io::Result<()> {
        let tmp = dbfile::temp_db_path(&self.path, "series-compact");
        dbfile::create_new_db_file(&tmp)?;

        dbfile::iter_records(&self.path, |hdr, payload| {
            if hdr.record_type != dbfile::RECORD_SERIES_SEGMENT {
                dbfile::append_record(&tmp, hdr.record_type, payload)?;
                return Ok(());
            }
            let (name, data) = decode_name_prefix(payload)?;
            if name != series {
                dbfile::append_record(&tmp, hdr.record_type, payload)?;
                return Ok(());
            }
            let seg = read_series_segment_from_bytes(data)?;
            let mut kept: Vec<Point> = Vec::new();
            kept.reserve(seg.count);
            for i in 0..seg.count {
                if seg.ts_ms[i] < cutoff_ms {
                    continue;
                }
                kept.push(Point {
                    ts_ms: seg.ts_ms[i],
                    value: seg.values[i],
                });
            }
            if kept.is_empty() {
                return Ok(());
            }
            let rebuilt = encode_series_segment(&kept, seg.min_seq, seg.max_seq)?;
            let payload = encode_named_payload(&name, &rebuilt)?;
            dbfile::append_record(&tmp, dbfile::RECORD_SERIES_SEGMENT, &payload)?;
            Ok(())
        })?;

        std::fs::rename(&tmp, &self.path)?;
        let new_state = scan_db_file(&self.path)?;
        *self.state.lock().unwrap() = new_state;
        self.write_footer()?;
        Ok(())
    }

    pub fn read_table_columns_in_range(
        &self,
        table: &str,
        schema: &TableSchema,
        start_ms: i64,
        end_ms: i64,
    ) -> io::Result<(Vec<i64>, Vec<ColumnData>)> {
        self.read_table_columns_in_range_filtered(table, schema, start_ms, end_ms, &[])
    }

    pub fn read_table_columns_in_range_filtered(
        &self,
        table: &str,
        schema: &TableSchema,
        start_ms: i64,
        end_ms: i64,
        predicates: &[ColumnPredicate],
    ) -> io::Result<(Vec<i64>, Vec<ColumnData>)> {
        let index = self.load_or_rebuild_table_index(table)?;
        if index.is_empty() {
            let cols = schema
                .columns
                .iter()
                .map(|col| match col.col_type {
                    ColumnType::F64 => ColumnData::F64(Vec::new()),
                    ColumnType::I64 => ColumnData::I64(Vec::new()),
                    ColumnType::Bool => ColumnData::Bool(Vec::new()),
                    ColumnType::Utf8 => ColumnData::Utf8(Vec::new()),
                })
                .collect();
            return Ok((Vec::new(), cols));
        }
        let file = File::open(&self.path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let mut out_ts: Vec<i64> = Vec::new();
        let mut out_cols: Vec<ColumnData> = schema
            .columns
            .iter()
            .map(|col| match col.col_type {
                ColumnType::F64 => ColumnData::F64(Vec::new()),
                ColumnType::I64 => ColumnData::I64(Vec::new()),
                ColumnType::Bool => ColumnData::Bool(Vec::new()),
                ColumnType::Utf8 => ColumnData::Utf8(Vec::new()),
            })
            .collect();
        let mapped_predicates = map_predicates(schema, predicates);

        for e in index {
            if e.max_ts < start_ms || e.min_ts > end_ms {
                continue;
            }
            if !mapped_predicates.is_empty() && !entry_may_match_predicates(&e, &mapped_predicates) {
                continue;
            }
            let off = e.offset as usize;
            let end = off.saturating_add(e.len as usize);
            if end > mmap.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "table index range out of bounds",
                ));
            }
            verify_table_segment_checksum(&mmap, table, &e)?;
            let seg = read_table_segment_from_bytes(&mmap[off..end], schema)?;
            for i in 0..seg.ts_ms.len() {
                let t = seg.ts_ms[i];
                if t < start_ms || t > end_ms {
                    continue;
                }
                out_ts.push(t);
                for (cidx, col) in seg.cols.iter().enumerate() {
                    match (&mut out_cols[cidx], col) {
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
        }

        Ok((out_ts, out_cols))
    }

    pub fn compact_table_retention(&self, table: &str, cutoff_ms: i64) -> io::Result<()> {
        self.pack_table_with_cutoff(table, 8192, Some(cutoff_ms))
    }

    pub fn pack_table(&self, table: &str, target_segment_points: usize) -> io::Result<()> {
        self.pack_table_with_cutoff(table, target_segment_points, None)
    }

    fn pack_table_with_cutoff(
        &self,
        table: &str,
        target_segment_points: usize,
        cutoff_ms: Option<i64>,
    ) -> io::Result<()> {
        let schema = self.read_table_schema(table)?;
        let index = self.load_or_rebuild_table_index(table)?;
        if index.is_empty() {
            return Ok(());
        }
        let file = File::open(&self.path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let mut acc_ts: Vec<i64> = Vec::new();
        let mut acc_cols: Vec<ColumnData> = schema
            .columns
            .iter()
            .map(|col| match col.col_type {
                ColumnType::F64 => ColumnData::F64(Vec::new()),
                ColumnType::I64 => ColumnData::I64(Vec::new()),
                ColumnType::Bool => ColumnData::Bool(Vec::new()),
                ColumnType::Utf8 => ColumnData::Utf8(Vec::new()),
            })
            .collect();
        let mut acc_min_seq: u64 = 0;
        let mut acc_max_seq: u64 = 0;
        let tmp = rewrite_db_without_table(&self.path, table)?;

        for e in index {
            let off = e.offset as usize;
            let end = off.saturating_add(e.len as usize);
            if end > mmap.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "table index range out of bounds",
                ));
            }
            verify_table_segment_checksum(&mmap, table, &e)?;
            let seg = read_table_segment_from_bytes(&mmap[off..end], &schema)?;

            for i in 0..seg.ts_ms.len() {
                let t = seg.ts_ms[i];
                if let Some(cut) = cutoff_ms {
                    if t < cut {
                        continue;
                    }
                }
                if acc_ts.is_empty() {
                    acc_min_seq = seg.min_seq;
                }
                acc_max_seq = seg.max_seq;
                acc_ts.push(t);
                for (cidx, col) in seg.cols.iter().enumerate() {
                    match (&mut acc_cols[cidx], col) {
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

                if acc_ts.len() >= target_segment_points {
                    let bytes = encode_table_segment(
                        &schema,
                        &acc_ts,
                        &acc_cols,
                        acc_min_seq,
                        acc_max_seq,
                    )?;
                    if !bytes.is_empty() {
                        let payload = encode_named_payload(table, &bytes)?;
                        dbfile::append_record(&tmp, dbfile::RECORD_TABLE_SEGMENT, &payload)?;
                    }
                    acc_ts.clear();
                    for c in &mut acc_cols {
                        match c {
                            ColumnData::F64(v) => v.clear(),
                            ColumnData::I64(v) => v.clear(),
                            ColumnData::Bool(v) => v.clear(),
                            ColumnData::Utf8(v) => v.clear(),
                        }
                    }
                }
            }
        }

        if !acc_ts.is_empty() {
            let bytes = encode_table_segment(&schema, &acc_ts, &acc_cols, acc_min_seq, acc_max_seq)?;
            if !bytes.is_empty() {
                let payload = encode_named_payload(table, &bytes)?;
                dbfile::append_record(&tmp, dbfile::RECORD_TABLE_SEGMENT, &payload)?;
            }
        }

        std::fs::rename(&tmp, &self.path)?;
        let new_state = scan_db_file(&self.path)?;
        *self.state.lock().unwrap() = new_state;
        self.write_table_index(table)?;
        self.write_footer()?;
        Ok(())
    }

    fn load_or_rebuild_table_index(&self, table: &str) -> io::Result<Vec<TableIndexEntry>> {
        {
            let state = self.state.lock().unwrap();
            if let Some(idx) = state.table_index.get(table) {
                return Ok(idx.clone());
            }
        }
        self.rebuild_table_index(table)
    }

    fn rebuild_table_index(&self, table: &str) -> io::Result<Vec<TableIndexEntry>> {
        let schema = self.read_table_schema(table)?;
        let mut idx = Vec::new();
        dbfile::iter_records(&self.path, |hdr, payload| {
            if hdr.record_type != dbfile::RECORD_TABLE_SEGMENT {
                return Ok(());
            }
            let (name, data) = decode_name_prefix(payload)?;
            if name != table {
                return Ok(());
            }
            let header = parse_table_segment_header(data)?;
            let seg = read_table_segment_from_bytes(data, &schema)?;
            let col_stats = column_stats_from_cols(&schema, &seg.cols);
            let name_len = name.as_bytes().len() as u64;
            let segment_offset = hdr.payload_offset + 2 + name_len;
            idx.push(TableIndexEntry {
                offset: segment_offset,
                len: data.len() as u64,
                min_ts: header.min_ts,
                max_ts: header.max_ts,
                min_seq: header.min_seq,
                max_seq: header.max_seq,
                count: header.count,
                col_stats,
            });
            Ok(())
        })?;
        {
            let mut state = self.state.lock().unwrap();
            state.table_index.insert(table.to_string(), idx.clone());
        }
        Ok(idx)
    }

    pub fn table_stats(&self, table: &str) -> io::Result<TableStats> {
        let schema = self.read_table_schema(table)?;
        let (schema_file_bytes, idx_file_bytes) = {
            let state = self.state.lock().unwrap();
            (
                state.schema_bytes.get(table).copied().unwrap_or(0),
                state.index_bytes.get(table).copied().unwrap_or(0),
            )
        };
        let index = self.load_or_rebuild_table_index(table)?;
        if index.is_empty() {
            let mut raw_value_bytes_per_row: u64 = 0;
            for col in &schema.columns {
                raw_value_bytes_per_row = raw_value_bytes_per_row.saturating_add(match col.col_type {
                    ColumnType::F64 | ColumnType::I64 => 8,
                    ColumnType::Bool => 1,
                    ColumnType::Utf8 => 0,
                });
            }
            return Ok(TableStats {
                rows: 0,
                segments: 0,
                columns: schema.columns.len() as u16,
                raw_ts_bytes: 0,
                raw_value_bytes: 0 * raw_value_bytes_per_row,
                stored_ts_bytes: 0,
                stored_value_bytes: 0,
                header_bytes: 0,
                ntt_file_bytes: 0,
                idx_file_bytes,
                schema_file_bytes,
            });
        }

        let file = File::open(&self.path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let mut rows: u64 = 0;
        let mut segments: u64 = 0;
        let mut stored_ts_bytes: u64 = 0;
        let mut stored_value_bytes: u64 = 0;
        let mut header_bytes: u64 = 0;
        let mut ntt_file_bytes: u64 = 0;

        for e in index {
            let off = e.offset as usize;
            let end = off.saturating_add(e.len as usize);
            if end > mmap.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "table index range out of bounds",
                ));
            }
            let (seg_rows, seg_ts_bytes, seg_val_bytes, seg_header_bytes) =
                table_segment_storage_stats(&mmap[off..end], schema.columns.len())?;
            rows = rows.saturating_add(seg_rows);
            segments = segments.saturating_add(1);
            stored_ts_bytes = stored_ts_bytes.saturating_add(seg_ts_bytes);
            stored_value_bytes = stored_value_bytes.saturating_add(seg_val_bytes);
            header_bytes = header_bytes.saturating_add(seg_header_bytes);
            ntt_file_bytes = ntt_file_bytes.saturating_add(e.len);
        }

        let columns = schema.columns.len() as u16;
        let mut raw_value_bytes_per_row: u64 = 0;
        for col in &schema.columns {
            raw_value_bytes_per_row = raw_value_bytes_per_row.saturating_add(match col.col_type {
                ColumnType::F64 | ColumnType::I64 => 8,
                ColumnType::Bool => 1,
                ColumnType::Utf8 => 0,
            });
        }
        Ok(TableStats {
            rows,
            segments,
            columns,
            raw_ts_bytes: rows.saturating_mul(8),
            raw_value_bytes: rows.saturating_mul(raw_value_bytes_per_row),
            stored_ts_bytes,
            stored_value_bytes,
            header_bytes,
            ntt_file_bytes,
            idx_file_bytes,
            schema_file_bytes,
        })
    }

    pub fn table_time_range(&self, table: &str) -> io::Result<Option<(i64, i64)>> {
        let _ = self.read_table_schema(table)?;
        let index = self.load_or_rebuild_table_index(table)?;
        if index.is_empty() {
            return Ok(None);
        }
        let mut min_ts = index[0].min_ts;
        let mut max_ts = index[0].max_ts;
        for e in index.iter().skip(1) {
            min_ts = min_ts.min(e.min_ts);
            max_ts = max_ts.max(e.max_ts);
        }
        Ok(Some((min_ts, max_ts)))
    }
}

#[derive(Debug, Clone)]
struct TableIndexEntry {
    offset: u64,
    len: u64,
    min_ts: i64,
    max_ts: i64,
    min_seq: u64,
    max_seq: u64,
    count: u32,
    col_stats: Vec<Option<ColumnStats>>,
}

#[derive(Debug, Clone)]
enum ColumnStats {
    F64 { min: f64, max: f64 },
    I64 { min: i64, max: i64 },
    Bool { min: bool, max: bool },
    Utf8 { min_len: u32, max_len: u32 },
}

#[derive(Debug)]
struct TableSegment {
    min_seq: u64,
    max_seq: u64,
    ts_ms: Vec<i64>,
    cols: Vec<ColumnData>,
}


fn read_table_segment_from_bytes(mut data: &[u8], schema: &TableSchema) -> io::Result<TableSegment> {
    fn read_exact<'a>(data: &mut &'a [u8], len: usize) -> io::Result<&'a [u8]> {
        if data.len() < len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment too short",
            ));
        }
        let (head, tail) = data.split_at(len);
        *data = tail;
        Ok(head)
    }

    let magic = read_exact(&mut data, 4)?;
    if magic != TABLE_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad table magic"));
    }
    let ver = read_exact(&mut data, 1)?[0];
    if ver != TABLE_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad table version"));
    }

    let min_seq = u64::from_le_bytes(read_exact(&mut data, 8)?.try_into().unwrap());
    let max_seq = u64::from_le_bytes(read_exact(&mut data, 8)?.try_into().unwrap());

    let count = u32::from_le_bytes(read_exact(&mut data, 4)?.try_into().unwrap()) as usize;
    let _min_ts = i64::from_le_bytes(read_exact(&mut data, 8)?.try_into().unwrap());
    let _max_ts = i64::from_le_bytes(read_exact(&mut data, 8)?.try_into().unwrap());
    let ext_len = u16::from_le_bytes(read_exact(&mut data, 2)?.try_into().unwrap()) as usize;
    if ext_len > 0 {
        let _ = read_exact(&mut data, ext_len)?;
    }

    let codec = read_exact(&mut data, 1)?[0];
    if codec != TS_CODEC_TS64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown ts codec (likely old layout; recreate DB)",
        ));
    }
    let ts_len = u32::from_le_bytes(read_exact(&mut data, 4)?.try_into().unwrap()) as usize;
    let ts_bytes = read_exact(&mut data, ts_len)?;
    let ts_ms = TimeSeriesCompressor::decompress(ts_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if ts_ms.len() != count {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "ts count mismatch"));
    }

    let ncols = u16::from_le_bytes(read_exact(&mut data, 2)?.try_into().unwrap()) as usize;
    if ncols != schema.columns.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "schema mismatch"));
    }

    let mut cols: Vec<ColumnData> = Vec::with_capacity(ncols);
    for col in &schema.columns {
        let null_len = u32::from_le_bytes(read_exact(&mut data, 4)?.try_into().unwrap()) as usize;
        let null_bytes = if null_len > 0 {
            read_exact(&mut data, null_len)?.to_vec()
        } else {
            Vec::new()
        };

        let ccodec = read_exact(&mut data, 1)?[0];
        let clen = u32::from_le_bytes(read_exact(&mut data, 4)?.try_into().unwrap()) as usize;
        let blob = read_exact(&mut data, clen)?;

        match col.col_type {
            ColumnType::F64 => {
                let values: Vec<f64> = match ccodec {
                    TABLE_COL_F64_XOR => {
                        decompress_f64_xor(blob, count)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    }
                    TABLE_COL_I64_D2 => {
                        let ints = TimeSeriesCompressor::decompress(blob)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        if ints.len() != count {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "col count mismatch",
                            ));
                        }
                        ints.into_iter().map(|x| x as f64).collect()
                    }
                    _ => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown col codec"))
                    }
                };
                if values.len() != count {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "col count mismatch"));
                }
                let mut out: Vec<Option<f64>> = Vec::with_capacity(count);
                for i in 0..count {
                    if null_bytes.is_empty() || is_valid(&null_bytes, i) {
                        out.push(Some(values[i]));
                    } else {
                        out.push(None);
                    }
                }
                cols.push(ColumnData::F64(out));
            }
            ColumnType::I64 => {
                if ccodec != TABLE_COL_I64_D2 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown col codec"));
                }
                let ints = TimeSeriesCompressor::decompress(blob)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if ints.len() != count {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "col count mismatch"));
                }
                let mut out: Vec<Option<i64>> = Vec::with_capacity(count);
                for i in 0..count {
                    if null_bytes.is_empty() || is_valid(&null_bytes, i) {
                        out.push(Some(ints[i]));
                    } else {
                        out.push(None);
                    }
                }
                cols.push(ColumnData::I64(out));
            }
            ColumnType::Bool => {
                if ccodec != TABLE_COL_BOOL {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown col codec"));
                }
                let expected = (count + 7) / 8;
                if blob.len() != expected {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "col count mismatch"));
                }
                let mut out: Vec<Option<bool>> = Vec::with_capacity(count);
                for i in 0..count {
                    if null_bytes.is_empty() || is_valid(&null_bytes, i) {
                        let byte = i / 8;
                        let bit = i % 8;
                        let value = ((blob[byte] >> bit) & 1) == 1;
                        out.push(Some(value));
                    } else {
                        out.push(None);
                    }
                }
                cols.push(ColumnData::Bool(out));
            }
            ColumnType::Utf8 => {
                if ccodec != TABLE_COL_UTF8 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown col codec"));
                }
                let need_offsets = (count + 1)
                    .checked_mul(4)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "overflow"))?;
                if blob.len() < need_offsets {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "short utf8 column",
                    ));
                }
                let mut offsets: Vec<u32> = Vec::with_capacity(count + 1);
                for i in 0..=count {
                    let start = i * 4;
                    let off = u32::from_le_bytes(blob[start..start + 4].try_into().unwrap());
                    offsets.push(off);
                }
                let data_bytes = &blob[need_offsets..];
                let mut out: Vec<Option<String>> = Vec::with_capacity(count);
                for i in 0..count {
                    if !null_bytes.is_empty() && !is_valid(&null_bytes, i) {
                        out.push(None);
                        continue;
                    }
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    if start > end || end > data_bytes.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "bad utf8 offsets",
                        ));
                    }
                    let s = std::str::from_utf8(&data_bytes[start..end]).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "bad utf8 data")
                    })?;
                    out.push(Some(s.to_string()));
                }
                cols.push(ColumnData::Utf8(out));
            }
        }
    }

    Ok(TableSegment {
        min_seq,
        max_seq,
        ts_ms,
        cols,
    })
}

fn verify_table_segment_checksum(
    mmap: &[u8],
    table: &str,
    entry: &TableIndexEntry,
) -> io::Result<()> {
    let name_len = table.as_bytes().len() as u64;
    let payload_len_expected = 2u64
        .saturating_add(name_len)
        .saturating_add(entry.len);
    let payload_offset = entry
        .offset
        .checked_sub(2u64.saturating_add(name_len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "table payload offset underflow"))?;
    let record_offset = payload_offset
        .checked_sub(5)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record offset underflow"))?;
    let end = record_offset
        .saturating_add(5)
        .saturating_add(payload_len_expected)
        .saturating_add(4);
    if end as usize > mmap.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record checksum out of bounds",
        ));
    }
    let record_type = mmap[record_offset as usize];
    if record_type != dbfile::RECORD_TABLE_SEGMENT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record type mismatch for table segment",
        ));
    }
    let len_bytes: [u8; 4] = mmap[(record_offset + 1) as usize..(record_offset + 5) as usize]
        .try_into()
        .unwrap();
    let payload_len = u32::from_le_bytes(len_bytes) as u64;
    if payload_len != payload_len_expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record payload length mismatch",
        ));
    }
    let payload_start = payload_offset as usize;
    let payload_end = payload_start + payload_len_expected as usize;
    let mut verify = Vec::with_capacity(1 + 4 + payload_len_expected as usize);
    verify.push(record_type);
    verify.extend_from_slice(&(payload_len as u32).to_le_bytes());
    verify.extend_from_slice(&mmap[payload_start..payload_end]);
    let checksum_offset = payload_end;
    let checksum_bytes: [u8; 4] = mmap[checksum_offset..checksum_offset + 4]
        .try_into()
        .unwrap();
    let checksum = u32::from_le_bytes(checksum_bytes);
    if dbfile::fnv1a32(&verify) != checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record checksum mismatch",
        ));
    }
    Ok(())
}


fn compress_f64_column_auto(col: &[Option<f64>]) -> (u8, Vec<u8>) {
    if let Some(ints) = try_f64_column_as_i64(col) {
        return (TABLE_COL_I64_D2, TimeSeriesCompressor::compress(&ints));
    }
    let mut raw: Vec<f64> = Vec::with_capacity(col.len());
    for v in col {
        raw.push(v.unwrap_or(0.0));
    }
    (TABLE_COL_F64_XOR, compress_f64_xor(&raw))
}

fn try_f64_column_as_i64(col: &[Option<f64>]) -> Option<Vec<i64>> {
    if col.is_empty() {
        return Some(Vec::new());
    }
    let mut out: Vec<i64> = Vec::with_capacity(col.len());
    for v in col {
        match v {
            None => out.push(0),
            Some(v) => {
                if !v.is_finite() {
                    return None;
                }
                // Treat -0.0 as non-integer to preserve bitwise roundtrip behavior.
                if *v == 0.0 && v.is_sign_negative() {
                    return None;
                }
                // Require exact integer representable as i64 -> f64 roundtrip.
                if v.fract() != 0.0 {
                    return None;
                }
                if *v < (i64::MIN as f64) || *v > (i64::MAX as f64) {
                    return None;
                }
                let i = *v as i64;
                if (i as f64) != *v {
                    return None;
                }
                out.push(i);
            }
        }
    }
    Some(out)
}

fn build_null_bitmap<T>(values: &[Option<T>]) -> Option<Vec<u8>> {
    if values.is_empty() {
        return None;
    }
    let mut any_null = false;
    let mut out = vec![0u8; (values.len() + 7) / 8];
    for (i, v) in values.iter().enumerate() {
        if v.is_some() {
            let byte = i / 8;
            let bit = i % 8;
            out[byte] |= 1u8 << bit;
        } else {
            any_null = true;
        }
    }
    if any_null {
        Some(out)
    } else {
        None
    }
}

fn checked_u32_len(len: usize, what: &str) -> io::Result<u32> {
    if len > u32::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, what));
    }
    Ok(len as u32)
}

fn column_stats_from_cols(schema: &TableSchema, cols: &[ColumnData]) -> Vec<Option<ColumnStats>> {
    let mut out = Vec::with_capacity(schema.columns.len());
    for (col_schema, col) in schema.columns.iter().zip(cols.iter()) {
        let stats = match (col_schema.col_type, col) {
            (ColumnType::F64, ColumnData::F64(values)) => stats_f64(values),
            (ColumnType::I64, ColumnData::I64(values)) => stats_i64(values),
            (ColumnType::Bool, ColumnData::Bool(values)) => stats_bool(values),
            (ColumnType::Utf8, ColumnData::Utf8(values)) => stats_utf8(values),
            _ => None,
        };
        out.push(stats);
    }
    out
}

fn stats_f64(values: &[Option<f64>]) -> Option<ColumnStats> {
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    for v in values {
        let v = match v {
            Some(v) => *v,
            None => continue,
        };
        if v.is_nan() {
            return None;
        }
        min = Some(match min {
            Some(cur) => cur.min(v),
            None => v,
        });
        max = Some(match max {
            Some(cur) => cur.max(v),
            None => v,
        });
    }
    min.zip(max)
        .map(|(min, max)| ColumnStats::F64 { min, max })
}

fn stats_i64(values: &[Option<i64>]) -> Option<ColumnStats> {
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for v in values {
        let v = match v {
            Some(v) => *v,
            None => continue,
        };
        min = Some(match min {
            Some(cur) => cur.min(v),
            None => v,
        });
        max = Some(match max {
            Some(cur) => cur.max(v),
            None => v,
        });
    }
    min.zip(max)
        .map(|(min, max)| ColumnStats::I64 { min, max })
}

fn stats_bool(values: &[Option<bool>]) -> Option<ColumnStats> {
    let mut seen_true = false;
    let mut seen_false = false;
    for v in values {
        match v {
            Some(true) => seen_true = true,
            Some(false) => seen_false = true,
            None => {}
        }
    }
    if !seen_true && !seen_false {
        return None;
    }
    let min = if seen_false { false } else { true };
    let max = if seen_true { true } else { false };
    Some(ColumnStats::Bool { min, max })
}

fn stats_utf8(values: &[Option<String>]) -> Option<ColumnStats> {
    let mut min: Option<u32> = None;
    let mut max: Option<u32> = None;
    for v in values {
        let s = match v {
            Some(s) => s,
            None => continue,
        };
        let len = s.len();
        let len = match checked_u32_len(len, "utf8 length exceeds u32::MAX") {
            Ok(len) => len,
            Err(_) => return None,
        };
        min = Some(match min {
            Some(cur) => cur.min(len),
            None => len,
        });
        max = Some(match max {
            Some(cur) => cur.max(len),
            None => len,
        });
    }
    min.zip(max)
        .map(|(min_len, max_len)| ColumnStats::Utf8 { min_len, max_len })
}

#[derive(Debug, Clone)]
struct MappedPredicate {
    col_idx: usize,
    op: ColumnPredicateOp,
    value: ColumnPredicateValue,
}

fn map_predicates(schema: &TableSchema, predicates: &[ColumnPredicate]) -> Vec<MappedPredicate> {
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

fn entry_may_match_predicates(entry: &TableIndexEntry, preds: &[MappedPredicate]) -> bool {
    for pred in preds {
        let stats = match entry.col_stats.get(pred.col_idx) {
            Some(stats) => stats,
            None => return true,
        };
        let stats = match stats {
            Some(stats) => stats,
            None => return true,
        };
        let ok = match (stats, &pred.value) {
            (ColumnStats::F64 { min, max }, ColumnPredicateValue::F64(v)) => {
                range_may_match(*min, *max, *v, pred.op)
            }
            (ColumnStats::I64 { min, max }, ColumnPredicateValue::I64(v)) => {
                range_may_match(*min, *max, *v, pred.op)
            }
            (ColumnStats::Bool { min, max }, ColumnPredicateValue::Bool(v)) => {
                bool_range_may_match(*min, *max, *v, pred.op)
            }
            _ => true,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn range_may_match<T: PartialOrd + Copy>(min: T, max: T, value: T, op: ColumnPredicateOp) -> bool {
    match op {
        ColumnPredicateOp::Eq => value >= min && value <= max,
        ColumnPredicateOp::Gt => max > value,
        ColumnPredicateOp::GtEq => max >= value,
        ColumnPredicateOp::Lt => min < value,
        ColumnPredicateOp::LtEq => min <= value,
    }
}

fn bool_range_may_match(min: bool, max: bool, value: bool, op: ColumnPredicateOp) -> bool {
    let min = min as u8;
    let max = max as u8;
    let value = value as u8;
    match op {
        ColumnPredicateOp::Eq => value >= min && value <= max,
        ColumnPredicateOp::Gt => max > value,
        ColumnPredicateOp::GtEq => max >= value,
        ColumnPredicateOp::Lt => min < value,
        ColumnPredicateOp::LtEq => min <= value,
    }
}

fn is_valid(bitmap: &[u8], idx: usize) -> bool {
    let byte = idx / 8;
    let bit = idx % 8;
    if byte >= bitmap.len() {
        return false;
    }
    (bitmap[byte] >> bit) & 1 == 1
}

fn encode_table_schema_bytes(schema: &TableSchema) -> io::Result<Vec<u8>> {
    if schema.columns.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "schema must have at least 1 column",
        ));
    }
    if schema.columns.len() > u16::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "too many columns"));
    }

    let mut out = Vec::new();
    out.extend_from_slice(SCHEMA_MAGIC);
    out.push(SCHEMA_VERSION);
    out.extend_from_slice(&(schema.columns.len() as u16).to_le_bytes());
    for col in &schema.columns {
        let b = col.name.as_bytes();
        if b.len() > u16::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "column name too long"));
        }
        out.extend_from_slice(&(b.len() as u16).to_le_bytes());
        out.extend_from_slice(b);
        out.push(col.col_type.to_tag());
    }
    Ok(out)
}

fn decode_table_schema_bytes(data: &[u8]) -> io::Result<TableSchema> {
    if data.len() < 7 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "short schema header"));
    }
    if &data[0..4] != SCHEMA_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad schema header"));
    }
    let version = data[4];
    if version != SCHEMA_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad schema header"));
    }
    let mut pos = 5usize;
    let ncols = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        if data.len() < pos + 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short schema"));
        }
        let len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if data.len() < pos + len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short schema"));
        }
        let b = data[pos..pos + len].to_vec();
        pos += len;
        let s = String::from_utf8(b)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad column utf-8"))?;
        if data.len() <= pos {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short schema"));
        }
        let tag = data[pos];
        pos += 1;
        let col_type = ColumnType::from_tag(tag)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad column type"))?;
        columns.push(ColumnSchema {
            name: s,
            col_type,
        });
    }
    Ok(TableSchema { columns })
}

fn encode_named_payload(name: &str, data: &[u8]) -> io::Result<Vec<u8>> {
    let name_bytes = name.as_bytes();
    if name_bytes.len() > u16::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "name too long"));
    }
    let mut out = Vec::with_capacity(2 + name_bytes.len() + data.len());
    out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(name_bytes);
    out.extend_from_slice(data);
    Ok(out)
}

fn decode_name_prefix(payload: &[u8]) -> io::Result<(String, &[u8])> {
    if payload.len() < 2 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "short name prefix"));
    }
    let len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    if payload.len() < 2 + len {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "short name bytes"));
    }
    let name = std::str::from_utf8(&payload[2..2 + len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid name utf-8"))?
        .to_string();
    Ok((name, &payload[2 + len..]))
}

fn encode_table_segment(
    schema: &TableSchema,
    ts_ms: &[i64],
    cols: &[ColumnData],
    min_seq: u64,
    max_seq: u64,
) -> io::Result<Vec<u8>> {
    if ts_ms.is_empty() {
        return Ok(Vec::new());
    }
    if cols.len() != schema.columns.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "schema mismatch"));
    }
    for c in cols {
        let len = match c {
            ColumnData::F64(v) => v.len(),
            ColumnData::I64(v) => v.len(),
            ColumnData::Bool(v) => v.len(),
            ColumnData::Utf8(v) => v.len(),
        };
        if len != ts_ms.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "column length mismatch"));
        }
    }

    let count = checked_u32_len(ts_ms.len(), "row count exceeds u32::MAX")?;
    let mut min_ts = ts_ms[0];
    let mut max_ts = ts_ms[0];
    for &t in ts_ms {
        min_ts = min_ts.min(t);
        max_ts = max_ts.max(t);
    }
    let ts_bytes = TimeSeriesCompressor::compress(ts_ms);
    let ts_len = checked_u32_len(ts_bytes.len(), "ts bytes exceed u32::MAX")?;

    let mut out = Vec::new();
    out.extend_from_slice(TABLE_MAGIC);
    out.push(TABLE_VERSION);
    out.extend_from_slice(&min_seq.to_le_bytes());
    out.extend_from_slice(&max_seq.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&min_ts.to_le_bytes());
    out.extend_from_slice(&max_ts.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.push(TS_CODEC_TS64);
    out.extend_from_slice(&ts_len.to_le_bytes());
    out.extend_from_slice(&ts_bytes);
    out.extend_from_slice(&(schema.columns.len() as u16).to_le_bytes());
    for (col, schema_col) in cols.iter().zip(schema.columns.iter()) {
        match (col, schema_col.col_type) {
            (ColumnData::F64(values), ColumnType::F64) => {
                let nulls = build_null_bitmap(values);
                let (codec, blob) = compress_f64_column_auto(values);
                if let Some(nulls) = nulls {
                    let null_len = checked_u32_len(nulls.len(), "null bitmap exceeds u32::MAX")?;
                    out.extend_from_slice(&null_len.to_le_bytes());
                    out.extend_from_slice(&nulls);
                } else {
                    out.extend_from_slice(&0u32.to_le_bytes());
                }
                out.push(codec);
                let blob_len = checked_u32_len(blob.len(), "column bytes exceed u32::MAX")?;
                out.extend_from_slice(&blob_len.to_le_bytes());
                out.extend_from_slice(&blob);
            }
            (ColumnData::I64(values), ColumnType::I64) => {
                let nulls = build_null_bitmap(values);
                let mut raw: Vec<i64> = Vec::with_capacity(values.len());
                for v in values {
                    raw.push(v.unwrap_or(0));
                }
                let blob = TimeSeriesCompressor::compress(&raw);
                if let Some(nulls) = nulls {
                    let null_len = checked_u32_len(nulls.len(), "null bitmap exceeds u32::MAX")?;
                    out.extend_from_slice(&null_len.to_le_bytes());
                    out.extend_from_slice(&nulls);
                } else {
                    out.extend_from_slice(&0u32.to_le_bytes());
                }
                out.push(TABLE_COL_I64_D2);
                let blob_len = checked_u32_len(blob.len(), "column bytes exceed u32::MAX")?;
                out.extend_from_slice(&blob_len.to_le_bytes());
                out.extend_from_slice(&blob);
            }
            (ColumnData::Bool(values), ColumnType::Bool) => {
                let nulls = build_null_bitmap(values);
                let mut raw = vec![0u8; (values.len() + 7) / 8];
                for (i, v) in values.iter().enumerate() {
                    if v.unwrap_or(false) {
                        let byte = i / 8;
                        let bit = i % 8;
                        raw[byte] |= 1u8 << bit;
                    }
                }
                if let Some(nulls) = nulls {
                    let null_len = checked_u32_len(nulls.len(), "null bitmap exceeds u32::MAX")?;
                    out.extend_from_slice(&null_len.to_le_bytes());
                    out.extend_from_slice(&nulls);
                } else {
                    out.extend_from_slice(&0u32.to_le_bytes());
                }
                out.push(TABLE_COL_BOOL);
                let raw_len = checked_u32_len(raw.len(), "bool bytes exceed u32::MAX")?;
                out.extend_from_slice(&raw_len.to_le_bytes());
                out.extend_from_slice(&raw);
            }
            (ColumnData::Utf8(values), ColumnType::Utf8) => {
                let nulls = build_null_bitmap(values);
                let mut offsets: Vec<u32> = Vec::with_capacity(values.len() + 1);
                let mut data_bytes: Vec<u8> = Vec::new();
                offsets.push(0);
                for v in values {
                    if let Some(s) = v {
                        let bytes = s.as_bytes();
                        if data_bytes.len() + bytes.len() > u32::MAX as usize {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "utf8 data exceeds u32::MAX",
                            ));
                        }
                        data_bytes.extend_from_slice(bytes);
                    }
                    offsets.push(data_bytes.len() as u32);
                }
                let offsets_bytes_len = offsets
                    .len()
                    .checked_mul(4)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "overflow"))?;
                let blob_len = offsets_bytes_len
                    .checked_add(data_bytes.len())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "overflow"))?;
                let blob_len_u32 =
                    checked_u32_len(blob_len, "utf8 column bytes exceed u32::MAX")?;
                let mut blob = Vec::with_capacity(blob_len);
                for off in offsets {
                    blob.extend_from_slice(&off.to_le_bytes());
                }
                blob.extend_from_slice(&data_bytes);
                if let Some(nulls) = nulls {
                    let null_len = checked_u32_len(nulls.len(), "null bitmap exceeds u32::MAX")?;
                    out.extend_from_slice(&null_len.to_le_bytes());
                    out.extend_from_slice(&nulls);
                } else {
                    out.extend_from_slice(&0u32.to_le_bytes());
                }
                out.push(TABLE_COL_UTF8);
                out.extend_from_slice(&blob_len_u32.to_le_bytes());
                out.extend_from_slice(&blob);
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "schema mismatch"));
            }
        }
    }
    Ok(out)
}

fn table_segment_storage_stats(
    data: &[u8],
    expected_cols: usize,
) -> io::Result<(u64, u64, u64, u64)> {
    let mut pos = 0usize;
    let mut read_exact = |len: usize| -> io::Result<&[u8]> {
        if data.len() < pos + len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment too short",
            ));
        }
        let out = &data[pos..pos + len];
        pos += len;
        Ok(out)
    };

    let magic = read_exact(4)?;
    if magic != TABLE_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad table magic"));
    }
    let ver = read_exact(1)?[0];
    if ver != TABLE_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad table version"));
    }

    read_exact(8)?; // min_seq
    read_exact(8)?; // max_seq
    let count = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as u64;
    read_exact(8)?; // min_ts
    read_exact(8)?; // max_ts
    let ext_len = u16::from_le_bytes(read_exact(2)?.try_into().unwrap()) as usize;
    if ext_len > 0 {
        let _ = read_exact(ext_len)?;
    }

    let mut header_bytes = (4 + 1 + 8 + 8 + 4 + 8 + 8 + 2 + ext_len) as u64;
    let codec = read_exact(1)?[0];
    header_bytes += 1;
    if codec != TS_CODEC_TS64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown ts codec (likely old layout; recreate DB)",
        ));
    }
    let ts_len = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as u64;
    header_bytes += 4;
    let _ = read_exact(ts_len as usize)?;

    let ncols = u16::from_le_bytes(read_exact(2)?.try_into().unwrap()) as usize;
    header_bytes += 2;
    if ncols != expected_cols {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "schema mismatch"));
    }

    let mut stored_value_bytes = 0u64;
    for _ in 0..ncols {
        let null_len = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as u64;
        header_bytes += 4 + null_len;
        let _ = read_exact(null_len as usize)?;
        let ccodec = read_exact(1)?[0];
        header_bytes += 1;
        if ccodec != TABLE_COL_F64_XOR
            && ccodec != TABLE_COL_I64_D2
            && ccodec != TABLE_COL_BOOL
            && ccodec != TABLE_COL_UTF8
        {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown col codec"));
        }
        let clen = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as u64;
        header_bytes += 4;
        stored_value_bytes += clen;
        let _ = read_exact(clen as usize)?;
    }

    Ok((count, ts_len, stored_value_bytes, header_bytes))
}

#[derive(Debug)]
struct TableSegmentHeader {
    min_seq: u64,
    max_seq: u64,
    count: u32,
    min_ts: i64,
    max_ts: i64,
}

fn parse_table_segment_header(data: &[u8]) -> io::Result<TableSegmentHeader> {
    if data.len() < 4 + 1 + 8 + 8 + 4 + 8 + 8 + 2 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "segment too short"));
    }
    if &data[0..4] != TABLE_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad table magic"));
    }
    if data[4] != TABLE_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad table version"));
    }
    let min_seq = u64::from_le_bytes(data[5..13].try_into().unwrap());
    let max_seq = u64::from_le_bytes(data[13..21].try_into().unwrap());
    let count = u32::from_le_bytes(data[21..25].try_into().unwrap());
    let min_ts = i64::from_le_bytes(data[25..33].try_into().unwrap());
    let max_ts = i64::from_le_bytes(data[33..41].try_into().unwrap());
    Ok(TableSegmentHeader {
        min_seq,
        max_seq,
        count,
        min_ts,
        max_ts,
    })
}

#[derive(Debug)]
struct SeriesSegment {
    min_seq: u64,
    max_seq: u64,
    count: usize,
    min_ts: i64,
    max_ts: i64,
    ts_ms: Vec<i64>,
    values: Vec<f64>,
}

fn encode_series_segment(points: &[Point], min_seq: u64, max_seq: u64) -> io::Result<Vec<u8>> {
    if points.is_empty() {
        return Ok(Vec::new());
    }
    let count = checked_u32_len(points.len(), "point count exceeds u32::MAX")?;
    let mut min_ts = points[0].ts_ms;
    let mut max_ts = points[0].ts_ms;
    let mut ts: Vec<i64> = Vec::with_capacity(points.len());
    let mut values: Vec<u8> = Vec::with_capacity(points.len() * 8);
    for p in points {
        min_ts = min_ts.min(p.ts_ms);
        max_ts = max_ts.max(p.ts_ms);
        ts.push(p.ts_ms);
        values.extend_from_slice(&p.value.to_le_bytes());
    }
    let ts_bytes = TimeSeriesCompressor::compress(&ts);
    let ts_len = checked_u32_len(ts_bytes.len(), "ts bytes exceed u32::MAX")?;
    let values_len = checked_u32_len(values.len(), "values bytes exceed u32::MAX")?;

    let mut out = Vec::new();
    out.extend_from_slice(SEG_MAGIC);
    out.push(SEG_VERSION);
    out.extend_from_slice(&min_seq.to_le_bytes());
    out.extend_from_slice(&max_seq.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&min_ts.to_le_bytes());
    out.extend_from_slice(&max_ts.to_le_bytes());
    out.push(TS_CODEC_TS64);
    out.extend_from_slice(&ts_len.to_le_bytes());
    out.extend_from_slice(&ts_bytes);
    out.extend_from_slice(&values_len.to_le_bytes());
    out.extend_from_slice(&values);
    Ok(out)
}

fn read_series_segment_from_bytes(data: &[u8]) -> io::Result<SeriesSegment> {
    let mut pos = 0usize;
    let mut read_exact = |len: usize| -> io::Result<&[u8]> {
        if data.len() < pos + len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment too short",
            ));
        }
        let out = &data[pos..pos + len];
        pos += len;
        Ok(out)
    };

    let magic = read_exact(4)?;
    if magic != SEG_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad segment magic"));
    }
    let ver = read_exact(1)?[0];
    if ver != SEG_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad segment version"));
    }
    let min_seq = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
    let max_seq = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
    let count = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as usize;
    let min_ts = i64::from_le_bytes(read_exact(8)?.try_into().unwrap());
    let max_ts = i64::from_le_bytes(read_exact(8)?.try_into().unwrap());
    let codec = read_exact(1)?[0];
    if codec != TS_CODEC_TS64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown ts codec (likely old layout; recreate DB)",
        ));
    }
    let ts_len = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as usize;
    let ts_bytes = read_exact(ts_len)?;
    let ts_ms = TimeSeriesCompressor::decompress(ts_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let val_len = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as usize;
    let val_bytes = read_exact(val_len)?;

    if ts_ms.len() != count || val_bytes.len() != count * 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "segment length mismatch"));
    }
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let v = f64::from_le_bytes(val_bytes[i * 8..i * 8 + 8].try_into().unwrap());
        values.push(v);
    }

    Ok(SeriesSegment {
        min_seq,
        max_seq,
        count,
        min_ts,
        max_ts,
        ts_ms,
        values,
    })
}

fn encode_meta_payload(last_seq: u64, retention_ms: Option<i64>) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 1 + 8 + 8);
    out.extend_from_slice(META_MAGIC);
    out.push(META_VERSION);
    out.extend_from_slice(&last_seq.to_le_bytes());
    let retention_raw = retention_ms.unwrap_or(-1);
    out.extend_from_slice(&retention_raw.to_le_bytes());
    out
}

fn decode_meta_payload(payload: &[u8]) -> io::Result<(u64, Option<i64>)> {
    if payload.len() < 4 + 1 + 8 + 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "short meta payload"));
    }
    if &payload[0..4] != META_MAGIC || payload[4] != META_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad meta header"));
    }
    let last_seq = u64::from_le_bytes(payload[5..13].try_into().unwrap());
    let retention_raw = i64::from_le_bytes(payload[13..21].try_into().unwrap());
    let retention_ms = if retention_raw < 0 { None } else { Some(retention_raw) };
    Ok((last_seq, retention_ms))
}

#[derive(Debug)]
struct FooterPayload {
    meta_offset: Option<u64>,
    schema_offsets: HashMap<String, u64>,
    index_offsets: HashMap<String, u64>,
}

fn encode_footer_payload(
    meta_offset: Option<u64>,
    schema_offsets: &HashMap<String, u64>,
    index_offsets: &HashMap<String, u64>,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(FOOTER_MAGIC);
    out.push(FOOTER_VERSION);
    out.extend_from_slice(&meta_offset.unwrap_or(0).to_le_bytes());

    let mut schema_keys: Vec<&String> = schema_offsets.keys().collect();
    schema_keys.sort();
    let schema_count = checked_u32_len(schema_keys.len(), "schema count exceeds u32::MAX")?;
    out.extend_from_slice(&schema_count.to_le_bytes());
    for name in schema_keys {
        let name_bytes = name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "schema name too long for footer",
            ));
        }
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(name_bytes);
        let offset = schema_offsets.get(name).copied().unwrap_or(0);
        out.extend_from_slice(&offset.to_le_bytes());
    }

    let mut index_keys: Vec<&String> = index_offsets.keys().collect();
    index_keys.sort();
    let index_count = checked_u32_len(index_keys.len(), "index count exceeds u32::MAX")?;
    out.extend_from_slice(&index_count.to_le_bytes());
    for name in index_keys {
        let name_bytes = name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "index name too long for footer",
            ));
        }
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(name_bytes);
        let offset = index_offsets.get(name).copied().unwrap_or(0);
        out.extend_from_slice(&offset.to_le_bytes());
    }

    Ok(out)
}

fn decode_footer_payload(payload: &[u8]) -> io::Result<FooterPayload> {
    let mut pos = 0usize;
    let mut read_exact = |len: usize| -> io::Result<&[u8]> {
        if payload.len() < pos + len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "footer payload too short",
            ));
        }
        let out = &payload[pos..pos + len];
        pos += len;
        Ok(out)
    };

    let magic = read_exact(4)?;
    if magic != FOOTER_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad footer magic"));
    }
    let version = read_exact(1)?[0];
    if version != FOOTER_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad footer version"));
    }
    let meta_offset = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
    let meta_offset = if meta_offset == 0 { None } else { Some(meta_offset) };

    let schema_count = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as usize;
    let mut schema_offsets = HashMap::with_capacity(schema_count);
    for _ in 0..schema_count {
        let name_len = u16::from_le_bytes(read_exact(2)?.try_into().unwrap()) as usize;
        let name = std::str::from_utf8(read_exact(name_len)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid footer name"))?
            .to_string();
        let offset = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        schema_offsets.insert(name, offset);
    }

    let index_count = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as usize;
    let mut index_offsets = HashMap::with_capacity(index_count);
    for _ in 0..index_count {
        let name_len = u16::from_le_bytes(read_exact(2)?.try_into().unwrap()) as usize;
        let name = std::str::from_utf8(read_exact(name_len)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid footer name"))?
            .to_string();
        let offset = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        index_offsets.insert(name, offset);
    }

    Ok(FooterPayload {
        meta_offset,
        schema_offsets,
        index_offsets,
    })
}

fn encode_table_index_payload(
    entries: &[TableIndexEntry],
    schema: &TableSchema,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(TABLE_INDEX_MAGIC);
    out.push(TABLE_INDEX_VERSION);
    let count = checked_u32_len(entries.len(), "index entry count exceeds u32::MAX")?;
    out.extend_from_slice(&count.to_le_bytes());
    for e in entries {
        if e.col_stats.len() != schema.columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "column stats length mismatch",
            ));
        }
        out.extend_from_slice(&e.offset.to_le_bytes());
        out.extend_from_slice(&e.len.to_le_bytes());
        out.extend_from_slice(&e.min_ts.to_le_bytes());
        out.extend_from_slice(&e.max_ts.to_le_bytes());
        out.extend_from_slice(&e.min_seq.to_le_bytes());
        out.extend_from_slice(&e.max_seq.to_le_bytes());
        out.extend_from_slice(&e.count.to_le_bytes());
        for (col_schema, stats) in schema.columns.iter().zip(e.col_stats.iter()) {
            let has = stats.is_some() as u8;
            out.push(has);
            match col_schema.col_type {
                ColumnType::F64 => {
                    let (min, max) = match stats {
                        Some(ColumnStats::F64 { min, max }) => (*min, *max),
                        _ => (0.0, 0.0),
                    };
                    out.extend_from_slice(&min.to_le_bytes());
                    out.extend_from_slice(&max.to_le_bytes());
                }
                ColumnType::I64 => {
                    let (min, max) = match stats {
                        Some(ColumnStats::I64 { min, max }) => (*min, *max),
                        _ => (0, 0),
                    };
                    out.extend_from_slice(&min.to_le_bytes());
                    out.extend_from_slice(&max.to_le_bytes());
                }
                ColumnType::Bool => {
                    let (min, max) = match stats {
                        Some(ColumnStats::Bool { min, max }) => (*min as u8, *max as u8),
                        _ => (0, 0),
                    };
                    out.push(min);
                    out.push(max);
                }
                ColumnType::Utf8 => {
                    let (min_len, max_len) = match stats {
                        Some(ColumnStats::Utf8 { min_len, max_len }) => (*min_len, *max_len),
                        _ => (0, 0),
                    };
                    out.extend_from_slice(&min_len.to_le_bytes());
                    out.extend_from_slice(&max_len.to_le_bytes());
                }
            }
        }
    }
    Ok(out)
}

fn decode_table_index_payload(
    payload: &[u8],
    schema: &TableSchema,
) -> io::Result<Vec<TableIndexEntry>> {
    let mut pos = 0usize;
    let mut read_exact = |len: usize| -> io::Result<&[u8]> {
        if payload.len() < pos + len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "index payload too short",
            ));
        }
        let out = &payload[pos..pos + len];
        pos += len;
        Ok(out)
    };

    let magic = read_exact(4)?;
    if magic != TABLE_INDEX_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad index magic"));
    }
    let version = read_exact(1)?[0];
    if version != TABLE_INDEX_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad index version"));
    }
    let count = u32::from_le_bytes(read_exact(4)?.try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let offset = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        let len = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        let min_ts = i64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        let max_ts = i64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        let min_seq = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        let max_seq = u64::from_le_bytes(read_exact(8)?.try_into().unwrap());
        let count = u32::from_le_bytes(read_exact(4)?.try_into().unwrap());
        let mut col_stats = Vec::with_capacity(schema.columns.len());
        for col_schema in &schema.columns {
            let has_stats = read_exact(1)?[0] != 0;
            let stats = match col_schema.col_type {
                ColumnType::F64 => {
                    let min = f64::from_le_bytes(read_exact(8)?.try_into().unwrap());
                    let max = f64::from_le_bytes(read_exact(8)?.try_into().unwrap());
                    if has_stats {
                        Some(ColumnStats::F64 { min, max })
                    } else {
                        None
                    }
                }
                ColumnType::I64 => {
                    let min = i64::from_le_bytes(read_exact(8)?.try_into().unwrap());
                    let max = i64::from_le_bytes(read_exact(8)?.try_into().unwrap());
                    if has_stats {
                        Some(ColumnStats::I64 { min, max })
                    } else {
                        None
                    }
                }
                ColumnType::Bool => {
                    let min = read_exact(1)?[0] != 0;
                    let max = read_exact(1)?[0] != 0;
                    if has_stats {
                        Some(ColumnStats::Bool { min, max })
                    } else {
                        None
                    }
                }
                ColumnType::Utf8 => {
                    let min_len = u32::from_le_bytes(read_exact(4)?.try_into().unwrap());
                    let max_len = u32::from_le_bytes(read_exact(4)?.try_into().unwrap());
                    if has_stats {
                        Some(ColumnStats::Utf8 { min_len, max_len })
                    } else {
                        None
                    }
                }
            };
            col_stats.push(stats);
        }
        entries.push(TableIndexEntry {
            offset,
            len,
            min_ts,
            max_ts,
            min_seq,
            max_seq,
            count,
            col_stats,
        });
    }
    Ok(entries)
}

fn load_state_from_footer(path: &Path) -> io::Result<Option<StorageState>> {
    let footer_offset = dbfile::read_footer_offset(path)?;
    if footer_offset == 0 {
        return Ok(None);
    }
    let (footer_hdr, footer_payload) = match dbfile::read_record_at(path, footer_offset) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    if footer_hdr.record_type != dbfile::RECORD_FOOTER {
        return Ok(None);
    }
    let footer = decode_footer_payload(&footer_payload)?;
    let mut state = StorageState::default();

    if let Some(meta_offset) = footer.meta_offset {
        if let Ok((hdr, payload)) = dbfile::read_record_at(path, meta_offset) {
            if hdr.record_type == dbfile::RECORD_META {
                if let Ok((last_seq, _)) = decode_meta_payload(&payload) {
                    state.max_seq = state.max_seq.max(last_seq);
                    state.meta_offset = Some(meta_offset);
                }
            }
        }
    }

    for (_name, offset) in footer.schema_offsets {
        if offset == 0 {
            continue;
        }
        let (hdr, payload) = dbfile::read_record_at(path, offset)?;
        if hdr.record_type != dbfile::RECORD_SCHEMA {
            continue;
        }
        let (table, data) = decode_name_prefix(&payload)?;
        let schema = decode_table_schema_bytes(data)?;
        state.schema_bytes.insert(table.clone(), data.len() as u64);
        state.schema_offsets.insert(table.clone(), offset);
        state.schemas.insert(table, schema);
    }

    for (_name, offset) in footer.index_offsets {
        if offset == 0 {
            continue;
        }
        let (hdr, payload) = dbfile::read_record_at(path, offset)?;
        if hdr.record_type != dbfile::RECORD_TABLE_INDEX {
            continue;
        }
        let (table, data) = decode_name_prefix(&payload)?;
        let schema = match state.schemas.get(&table) {
            Some(schema) => schema.clone(),
            None => continue,
        };
        let entries = decode_table_index_payload(data, &schema)?;
        for entry in &entries {
            state.max_seq = state.max_seq.max(entry.min_seq).max(entry.max_seq);
        }
        state.table_index.insert(table.clone(), entries);
        state.index_bytes.insert(table.clone(), hdr.payload_len as u64);
        state.index_offsets.insert(table, offset);
    }

    Ok(Some(state))
}

fn scan_db_file(path: &Path) -> io::Result<StorageState> {
    let mut state = StorageState::default();
    dbfile::iter_records(path, |hdr, payload| {
        match hdr.record_type {
            dbfile::RECORD_SCHEMA => {
                let (name, data) = decode_name_prefix(payload)?;
                let schema = decode_table_schema_bytes(data)?;
                state.schema_bytes.insert(name.clone(), data.len() as u64);
                state.schema_offsets.insert(name.clone(), hdr.record_offset);
                state.schemas.insert(name, schema);
            }
            dbfile::RECORD_TABLE_SEGMENT => {
                let (name, data) = decode_name_prefix(payload)?;
                let header = parse_table_segment_header(data)?;
                let col_stats = state
                    .schemas
                    .get(&name)
                    .and_then(|schema| {
                        read_table_segment_from_bytes(data, schema)
                            .ok()
                            .map(|seg| column_stats_from_cols(schema, &seg.cols))
                    })
                    .unwrap_or_default();
                let name_len = name.as_bytes().len() as u64;
                let offset = hdr.payload_offset + 2 + name_len;
                state
                    .table_index
                    .entry(name)
                    .or_default()
                    .push(TableIndexEntry {
                        offset,
                        len: data.len() as u64,
                        min_ts: header.min_ts,
                        max_ts: header.max_ts,
                        min_seq: header.min_seq,
                        max_seq: header.max_seq,
                        count: header.count,
                        col_stats,
                    });
                state.max_seq = state.max_seq.max(header.min_seq).max(header.max_seq);
            }
            dbfile::RECORD_TABLE_INDEX => {
                let (name, data) = decode_name_prefix(payload)?;
                let schema = state
                    .schemas
                    .get(&name)
                    .cloned()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "schema missing"))?;
                let entries = decode_table_index_payload(data, &schema)?;
                for entry in &entries {
                    state.max_seq = state
                        .max_seq
                        .max(entry.min_seq)
                        .max(entry.max_seq);
                }
                state.table_index.insert(name.clone(), entries);
                state.index_bytes
                    .insert(name.clone(), hdr.payload_len as u64);
                state.index_offsets.insert(name, hdr.record_offset);
            }
            dbfile::RECORD_SERIES_SEGMENT => {
                let (_, data) = decode_name_prefix(payload)?;
                if data.len() >= 4 + 1 + 8 + 8 {
                    let min_seq = u64::from_le_bytes(data[5..13].try_into().unwrap());
                    let max_seq = u64::from_le_bytes(data[13..21].try_into().unwrap());
                    state.max_seq = state.max_seq.max(min_seq).max(max_seq);
                }
            }
            dbfile::RECORD_META => {
                if let Ok((last_seq, _)) = decode_meta_payload(payload) {
                    state.max_seq = state.max_seq.max(last_seq);
                    state.meta_offset = Some(hdr.record_offset);
                }
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(state)
}

fn rewrite_db_without_table(path: &Path, table: &str) -> io::Result<PathBuf> {
    let tmp = dbfile::temp_db_path(path, "pack");
    dbfile::create_new_db_file(&tmp)?;
    dbfile::iter_records(path, |hdr, payload| {
        if hdr.record_type == dbfile::RECORD_TABLE_SEGMENT {
            let (name, _) = decode_name_prefix(payload)?;
            if name == table {
                return Ok(());
            }
        }
        if hdr.record_type == dbfile::RECORD_TABLE_INDEX {
            let (name, _) = decode_name_prefix(payload)?;
            if name == table {
                return Ok(());
            }
        }
        if hdr.record_type == dbfile::RECORD_WAL
            || hdr.record_type == dbfile::RECORD_WAL_CHECKPOINT
        {
            return Ok(());
        }
        dbfile::append_record(&tmp, hdr.record_type, payload)?;
        Ok(())
    })?;
    Ok(tmp)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn fresh_db_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nanots_storage_test_{}_{}_{}.ntt",
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
    fn test_table_segment_roundtrip_xor() {
        let path = fresh_db_path("table_seg");
        let storage = Storage::open(&path).unwrap();
        let schema = TableSchema::f64_columns(&["a", "b"]);
        storage.write_table_schema("t", &schema).unwrap();

        let ts: Vec<i64> = (0..4096).map(|i| 1_000_000 + i as i64).collect();
        let col_a: Vec<f64> = (0..4096).map(|i| i as f64 * 0.1).collect();
        let col_b: Vec<f64> = (0..4096).map(|i| (i % 17) as f64).collect();
        let col_a: Vec<Option<f64>> = col_a.into_iter().map(Some).collect();
        let col_b: Vec<Option<f64>> = col_b.into_iter().map(Some).collect();
        storage
            .append_table_segment(
                "t",
                &schema,
                &ts,
                &[
                    ColumnData::F64(col_a.clone()),
                    ColumnData::F64(col_b.clone()),
                ],
                1,
                4096,
            )
            .unwrap();

        let (out_ts, out_cols) = storage
            .read_table_columns_in_range("t", &schema, ts[100], ts[200])
            .unwrap();
        assert_eq!(out_cols.len(), 2);
        match (&out_cols[0], &out_cols[1]) {
            (ColumnData::F64(a), ColumnData::F64(b)) => {
                assert_eq!(out_ts.len(), a.len());
                assert_eq!(out_ts.len(), b.len());
            }
            _ => panic!("unexpected column type"),
        }
        assert!(out_ts[0] >= ts[100] && *out_ts.last().unwrap() <= ts[200]);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_table_stats_basic() {
        let path = fresh_db_path("stats");
        let storage = Storage::open(&path).unwrap();
        let schema = TableSchema::f64_columns(&["a", "b"]);
        storage.write_table_schema("t", &schema).unwrap();

        let ts: Vec<i64> = (0..1024).map(|i| 123_000 + i as i64).collect();
        let col_a: Vec<f64> = (0..1024).map(|i| i as f64).collect();
        let col_b: Vec<f64> = (0..1024).map(|i| (i % 11) as f64).collect();
        let col_a: Vec<Option<f64>> = col_a.into_iter().map(Some).collect();
        let col_b: Vec<Option<f64>> = col_b.into_iter().map(Some).collect();
        storage
            .append_table_segment(
                "t",
                &schema,
                &ts,
                &[ColumnData::F64(col_a), ColumnData::F64(col_b)],
                1,
                1024,
            )
            .unwrap();

        let st = storage.table_stats("t").unwrap();
        assert_eq!(st.rows, 1024);
        assert_eq!(st.segments, 1);
        assert_eq!(st.columns, 2);
        assert_eq!(st.raw_ts_bytes, 1024 * 8);
        assert_eq!(st.raw_value_bytes, 1024 * 8 * 2);
        assert!(st.ntt_file_bytes > 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_integer_column_uses_i64_codec() {
        let path = fresh_db_path("i64_codec");
        let storage = Storage::open(&path).unwrap();
        let schema = TableSchema::f64_columns(&["value"]);
        storage.write_table_schema("t", &schema).unwrap();

        let ts: Vec<i64> = (0..1024).map(|i| 1000 + i as i64).collect();
        let col: Vec<f64> = (0..1024).map(|i| i as f64).collect();
        let col: Vec<Option<f64>> = col.into_iter().map(Some).collect();
        storage
            .append_table_segment("t", &schema, &ts, &[ColumnData::F64(col.clone())], 1, 1024)
            .unwrap();

        let idx = storage.load_or_rebuild_table_index("t").unwrap();
        let entry = idx.first().unwrap();
        let mmap = unsafe { MmapOptions::new().map(&File::open(&storage.path).unwrap()).unwrap() };
        let start = entry.offset as usize;
        let end = start + entry.len as usize;

        let mut data: &[u8] = &mmap[start..end];
        fn take<'a>(data: &mut &'a [u8], n: usize) -> &'a [u8] {
            let (h, t) = data.split_at(n);
            *data = t;
            h
        }

        assert_eq!(take(&mut data, 4), TABLE_MAGIC);
        let ver = take(&mut data, 1)[0];
        assert_eq!(ver, TABLE_VERSION);
        let _ = take(&mut data, 8 + 8 + 4 + 8 + 8); // seq/count/min/max
        let ext_len = u16::from_le_bytes(take(&mut data, 2).try_into().unwrap()) as usize;
        let _ = take(&mut data, ext_len);

        assert_eq!(take(&mut data, 1)[0], TS_CODEC_TS64);
        let ts_len = u32::from_le_bytes(take(&mut data, 4).try_into().unwrap()) as usize;
        let _ = take(&mut data, ts_len);

        let ncols = u16::from_le_bytes(take(&mut data, 2).try_into().unwrap());
        assert_eq!(ncols, 1);

        let null_len = u32::from_le_bytes(take(&mut data, 4).try_into().unwrap()) as usize;
        if null_len > 0 {
            let _ = take(&mut data, null_len);
        }
        let codec = take(&mut data, 1)[0];
        assert_eq!(codec, TABLE_COL_I64_D2);

        // Roundtrip check through the public read path.
        let (out_ts, out_cols) = storage
            .read_table_columns_in_range("t", &schema, ts[0], ts[ts.len() - 1])
            .unwrap();
        assert_eq!(out_ts.len(), ts.len());
        assert_eq!(out_cols.len(), 1);
        match &out_cols[0] {
            ColumnData::F64(values) => {
                assert_eq!(values.len(), ts.len());
                for (a, b) in values.iter().zip(col.iter()) {
                    assert_eq!(a.unwrap().to_bits(), b.unwrap().to_bits());
                }
            }
            _ => panic!("unexpected column type"),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_bool_column_bit_packed() {
        let path = fresh_db_path("bool_packed");
        let storage = Storage::open(&path).unwrap();
        let schema = TableSchema {
            columns: vec![ColumnSchema {
                name: "b".to_string(),
                col_type: ColumnType::Bool,
            }],
        };
        storage.write_table_schema("t", &schema).unwrap();

        let ts: Vec<i64> = (0..17).map(|i| 10_000 + i as i64).collect();
        let values: Vec<Option<bool>> = vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
        ];
        storage
            .append_table_segment(
                "t",
                &schema,
                &ts,
                &[ColumnData::Bool(values.clone())],
                1,
                17,
            )
            .unwrap();

        let idx = storage.load_or_rebuild_table_index("t").unwrap();
        let entry = idx.first().unwrap();
        let mmap = unsafe { MmapOptions::new().map(&File::open(&storage.path).unwrap()).unwrap() };
        let start = entry.offset as usize;
        let end = start + entry.len as usize;

        let mut data: &[u8] = &mmap[start..end];
        fn take<'a>(data: &mut &'a [u8], n: usize) -> &'a [u8] {
            let (h, t) = data.split_at(n);
            *data = t;
            h
        }

        assert_eq!(take(&mut data, 4), TABLE_MAGIC);
        let _ = take(&mut data, 1)[0];
        let _ = take(&mut data, 8 + 8 + 4 + 8 + 8); // seq/count/min/max
        let ext_len = u16::from_le_bytes(take(&mut data, 2).try_into().unwrap()) as usize;
        let _ = take(&mut data, ext_len);

        assert_eq!(take(&mut data, 1)[0], TS_CODEC_TS64);
        let ts_len = u32::from_le_bytes(take(&mut data, 4).try_into().unwrap()) as usize;
        let _ = take(&mut data, ts_len);

        let ncols = u16::from_le_bytes(take(&mut data, 2).try_into().unwrap());
        assert_eq!(ncols, 1);

        let null_len = u32::from_le_bytes(take(&mut data, 4).try_into().unwrap()) as usize;
        let _ = take(&mut data, null_len);
        let codec = take(&mut data, 1)[0];
        assert_eq!(codec, TABLE_COL_BOOL);
        let col_len = u32::from_le_bytes(take(&mut data, 4).try_into().unwrap()) as usize;
        assert_eq!(col_len, (values.len() + 7) / 8);

        let (out_ts, out_cols) = storage
            .read_table_columns_in_range("t", &schema, ts[0], ts[ts.len() - 1])
            .unwrap();
        assert_eq!(out_ts.len(), ts.len());
        match &out_cols[0] {
            ColumnData::Bool(out_vals) => assert_eq!(out_vals, &values),
            _ => panic!("unexpected column type"),
        }

        let _ = fs::remove_file(path);
    }
}
