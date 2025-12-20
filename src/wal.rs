// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::{ColumnData, ColumnType, Value};
use crate::dbfile;
use std::io;
use std::path::{Path, PathBuf};

const WAL_MAGIC: &[u8; 4] = b"NTWL";
const WAL_VERSION: u8 = 1;
const RECORD_APPEND: u8 = 1;
const RECORD_APPEND_ROW: u8 = 2;
const RECORD_APPEND_ROW_TYPED: u8 = 3;
const RECORD_APPEND_BATCH: u8 = 4;
const WAL_VAL_NULL: u8 = 0;
const WAL_VAL_F64: u8 = 1;
const WAL_VAL_I64: u8 = 2;
const WAL_VAL_BOOL: u8 = 3;
const WAL_VAL_UTF8: u8 = 4;

#[derive(Debug)]
pub struct Wal {
    db_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WalRecord<'a> {
    Append {
        seq: u64,
        series: &'a str,
        ts_ms: i64,
        value: f64,
    },
    AppendRow {
        seq: u64,
        table: &'a str,
        ts_ms: i64,
        values: &'a [f64],
    },
    AppendRowTyped {
        seq: u64,
        table: &'a str,
        ts_ms: i64,
        values: &'a [WalValue<'a>],
    },
    AppendBatch {
        seq_start: u64,
        table: &'a str,
        ts_ms: &'a [i64],
        cols: &'a [ColumnData],
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalRecordOwned {
    Append {
        seq: u64,
        series: String,
        ts_ms: i64,
        value: f64,
    },
    AppendRow {
        seq: u64,
        table: String,
        ts_ms: i64,
        values: Vec<f64>,
    },
    AppendRowTyped {
        seq: u64,
        table: String,
        ts_ms: i64,
        values: Vec<Value>,
    },
    AppendBatch {
        seq_start: u64,
        table: String,
        ts_ms: Vec<i64>,
        cols: Vec<ColumnData>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WalValue<'a> {
    Null,
    F64(f64),
    I64(i64),
    Bool(bool),
    Utf8(&'a str),
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            db_path: path.as_ref().to_path_buf(),
        })
    }

    pub fn append(&mut self, record: WalRecord<'_>) -> io::Result<()> {
        let _ = self.append_with_size(record)?;
        Ok(())
    }

    pub fn append_with_size(&mut self, record: WalRecord<'_>) -> io::Result<u64> {
        let mut payload = Vec::with_capacity(64);
        payload.extend_from_slice(WAL_MAGIC);
        payload.push(WAL_VERSION);

        match record {
            WalRecord::Append {
                seq,
                series,
                ts_ms,
                value,
            } => {
                payload.push(RECORD_APPEND);
                payload.extend_from_slice(&seq.to_le_bytes());
                let series_bytes = series.as_bytes();
                if series_bytes.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "series name too long",
                    ));
                }
                payload.extend_from_slice(&(series_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(series_bytes);
                payload.extend_from_slice(&ts_ms.to_le_bytes());
                payload.extend_from_slice(&value.to_le_bytes());
            }
            WalRecord::AppendRow {
                seq,
                table,
                ts_ms,
                values,
            } => {
                payload.push(RECORD_APPEND_ROW);
                payload.extend_from_slice(&seq.to_le_bytes());
                let table_bytes = table.as_bytes();
                if table_bytes.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "table name too long",
                    ));
                }
                if values.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "too many columns",
                    ));
                }
                payload.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(table_bytes);
                payload.extend_from_slice(&ts_ms.to_le_bytes());
                payload.extend_from_slice(&(values.len() as u16).to_le_bytes());
                for v in values {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
            }
            WalRecord::AppendRowTyped {
                seq,
                table,
                ts_ms,
                values,
            } => {
                payload.push(RECORD_APPEND_ROW_TYPED);
                payload.extend_from_slice(&seq.to_le_bytes());
                let table_bytes = table.as_bytes();
                if table_bytes.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "table name too long",
                    ));
                }
                if values.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "too many columns",
                    ));
                }
                payload.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(table_bytes);
                payload.extend_from_slice(&ts_ms.to_le_bytes());
                payload.extend_from_slice(&(values.len() as u16).to_le_bytes());
                for v in values {
                    match v {
                        WalValue::Null => payload.push(WAL_VAL_NULL),
                        WalValue::F64(x) => {
                            payload.push(WAL_VAL_F64);
                            payload.extend_from_slice(&x.to_le_bytes());
                        }
                        WalValue::I64(x) => {
                            payload.push(WAL_VAL_I64);
                            payload.extend_from_slice(&x.to_le_bytes());
                        }
                        WalValue::Bool(x) => {
                            payload.push(WAL_VAL_BOOL);
                            payload.push(u8::from(*x));
                        }
                        WalValue::Utf8(s) => {
                            payload.push(WAL_VAL_UTF8);
                            let bytes = s.as_bytes();
                            if bytes.len() > u32::MAX as usize {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "string too long",
                                ));
                            }
                            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                            payload.extend_from_slice(bytes);
                        }
                    }
                }
            }
            WalRecord::AppendBatch {
                seq_start,
                table,
                ts_ms,
                cols,
            } => {
                payload.push(RECORD_APPEND_BATCH);
                payload.extend_from_slice(&seq_start.to_le_bytes());
                let table_bytes = table.as_bytes();
                if table_bytes.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "table name too long",
                    ));
                }
                if ts_ms.len() > u32::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "batch row count too large",
                    ));
                }
                if cols.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "too many columns",
                    ));
                }
                payload.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(table_bytes);
                payload.extend_from_slice(&(ts_ms.len() as u32).to_le_bytes());
                payload.extend_from_slice(&(cols.len() as u16).to_le_bytes());
                for ts in ts_ms {
                    payload.extend_from_slice(&ts.to_le_bytes());
                }
                for col in cols {
                    match col {
                        ColumnData::F64(values) => {
                            payload.push(ColumnType::F64.to_tag());
                            encode_null_bitmap(values, &mut payload)?;
                            for v in values {
                                payload.extend_from_slice(&v.unwrap_or(0.0).to_le_bytes());
                            }
                        }
                        ColumnData::I64(values) => {
                            payload.push(ColumnType::I64.to_tag());
                            encode_null_bitmap(values, &mut payload)?;
                            for v in values {
                                payload.extend_from_slice(&v.unwrap_or(0).to_le_bytes());
                            }
                        }
                        ColumnData::Bool(values) => {
                            payload.push(ColumnType::Bool.to_tag());
                            encode_null_bitmap(values, &mut payload)?;
                            for v in values {
                                payload.push(u8::from(v.unwrap_or(false)));
                            }
                        }
                        ColumnData::Utf8(values) => {
                            payload.push(ColumnType::Utf8.to_tag());
                            encode_null_bitmap(values, &mut payload)?;
                            for v in values {
                                if let Some(s) = v {
                                    let bytes = s.as_bytes();
                                    if bytes.len() > u32::MAX as usize {
                                        return Err(io::Error::new(
                                            io::ErrorKind::InvalidInput,
                                            "string too long",
                                        ));
                                    }
                                    payload.extend_from_slice(
                                        &(bytes.len() as u32).to_le_bytes(),
                                    );
                                    payload.extend_from_slice(bytes);
                                } else {
                                    payload.extend_from_slice(&0u32.to_le_bytes());
                                }
                            }
                        }
                    }
                }
            }
        }

        let payload_len = payload.len() as u64;
        let _ = dbfile::append_record(&self.db_path, dbfile::RECORD_WAL, &payload)?;
        Ok(5 + payload_len + 4)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    pub fn reset(&mut self) -> io::Result<()> {
        let _ = dbfile::append_record(&self.db_path, dbfile::RECORD_WAL_CHECKPOINT, &[])?;
        Ok(())
    }

    pub fn replay<F>(&self, mut on_record: F) -> io::Result<()>
    where
        F: FnMut(WalRecordOwned) -> io::Result<()>,
    {
        let mut pending: Vec<WalRecordOwned> = Vec::new();
        dbfile::iter_records(&self.db_path, |hdr, payload| {
            match hdr.record_type {
                dbfile::RECORD_WAL_CHECKPOINT => {
                    pending.clear();
                }
                dbfile::RECORD_WAL => {
                    let rec = decode_wal_payload(payload)?;
                    pending.push(rec);
                }
                _ => {}
            }
            Ok(())
        })?;

        for rec in pending {
            on_record(rec)?;
        }
        Ok(())
    }

    pub fn bytes_since_checkpoint(&self) -> io::Result<u64> {
        let mut bytes: u64 = 0;
        dbfile::iter_records(&self.db_path, |hdr, _| {
            match hdr.record_type {
                dbfile::RECORD_WAL_CHECKPOINT => bytes = 0,
                dbfile::RECORD_WAL => {
                    bytes = bytes.saturating_add(5 + hdr.payload_len as u64 + 4);
                }
                _ => {}
            }
            Ok(())
        })?;
        Ok(bytes)
    }
}

fn decode_wal_payload(payload: &[u8]) -> io::Result<WalRecordOwned> {
    if payload.len() < 6 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "short WAL payload"));
    }
    if &payload[0..4] != WAL_MAGIC || payload[4] != WAL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid WAL record header",
        ));
    }
    fn read_exact<'a>(payload: &'a [u8], pos: &mut usize, len: usize) -> io::Result<&'a [u8]> {
        if payload.len() < pos.saturating_add(len) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "short WAL payload",
            ));
        }
        let out = &payload[*pos..*pos + len];
        *pos += len;
        Ok(out)
    }

    let mut pos = 5usize;
    let record_type = read_exact(payload, &mut pos, 1)?[0];
    let record = match record_type {
        RECORD_APPEND => {
            let seq = u64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
            let series_len =
                u16::from_le_bytes(read_exact(payload, &mut pos, 2)?.try_into().unwrap()) as usize;
            let series_bytes = read_exact(payload, &mut pos, series_len)?;
            let ts_ms =
                i64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
            let value =
                f64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());

            let series = std::str::from_utf8(series_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid series utf-8")
            })?;

            Ok(WalRecordOwned::Append {
                seq,
                series: series.to_string(),
                ts_ms,
                value,
            })
        }
        RECORD_APPEND_ROW => {
            let seq = u64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
            let table_len =
                u16::from_le_bytes(read_exact(payload, &mut pos, 2)?.try_into().unwrap()) as usize;
            let table_bytes = read_exact(payload, &mut pos, table_len)?;
            let ts_ms =
                i64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
            let ncols =
                u16::from_le_bytes(read_exact(payload, &mut pos, 2)?.try_into().unwrap()) as usize;

            let need = ncols
                .checked_mul(8)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "overflow"))?;
            if payload.len() < pos.saturating_add(need) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "short WAL row payload",
                ));
            }
            let mut values: Vec<f64> = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let v =
                    f64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
                values.push(v);
            }

            let table = std::str::from_utf8(table_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid table utf-8")
            })?;

            Ok(WalRecordOwned::AppendRow {
                seq,
                table: table.to_string(),
                ts_ms,
                values,
            })
        }
        RECORD_APPEND_ROW_TYPED => {
            let seq = u64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
            let table_len =
                u16::from_le_bytes(read_exact(payload, &mut pos, 2)?.try_into().unwrap()) as usize;
            let table_bytes = read_exact(payload, &mut pos, table_len)?;
            let ts_ms =
                i64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
            let ncols =
                u16::from_le_bytes(read_exact(payload, &mut pos, 2)?.try_into().unwrap()) as usize;

            let mut values: Vec<crate::db::Value> = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let tag = read_exact(payload, &mut pos, 1)?[0];
                match tag {
                    WAL_VAL_NULL => values.push(crate::db::Value::Null),
                    WAL_VAL_F64 => {
                        let v =
                            f64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
                        values.push(crate::db::Value::F64(v));
                    }
                    WAL_VAL_I64 => {
                        let v =
                            i64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
                        values.push(crate::db::Value::I64(v));
                    }
                    WAL_VAL_BOOL => {
                        let v = read_exact(payload, &mut pos, 1)?[0] != 0;
                        values.push(crate::db::Value::Bool(v));
                    }
                    WAL_VAL_UTF8 => {
                        let len =
                            u32::from_le_bytes(read_exact(payload, &mut pos, 4)?.try_into().unwrap())
                                as usize;
                        let s = std::str::from_utf8(read_exact(payload, &mut pos, len)?).map_err(
                            |_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8"),
                        )?;
                        values.push(crate::db::Value::Utf8(s.to_string()));
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unknown typed WAL value tag",
                        ))
                    }
                }
            }

            let table = std::str::from_utf8(table_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid table utf-8")
            })?;

            Ok(WalRecordOwned::AppendRowTyped {
                seq,
                table: table.to_string(),
                ts_ms,
                values,
            })
        }
        RECORD_APPEND_BATCH => {
            let seq_start =
                u64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
            let table_len =
                u16::from_le_bytes(read_exact(payload, &mut pos, 2)?.try_into().unwrap()) as usize;
            let table_bytes = read_exact(payload, &mut pos, table_len)?;
            let row_count =
                u32::from_le_bytes(read_exact(payload, &mut pos, 4)?.try_into().unwrap()) as usize;
            let ncols =
                u16::from_le_bytes(read_exact(payload, &mut pos, 2)?.try_into().unwrap()) as usize;
            let mut ts_ms = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let v =
                    i64::from_le_bytes(read_exact(payload, &mut pos, 8)?.try_into().unwrap());
                ts_ms.push(v);
            }
            let mut cols: Vec<ColumnData> = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let tag = read_exact(payload, &mut pos, 1)?[0];
                let col_type = ColumnType::from_tag(tag).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "unknown batch column tag")
                })?;
                let has_nulls = read_exact(payload, &mut pos, 1)?[0] != 0;
                let nulls = if has_nulls {
                    let bytes = (row_count + 7) / 8;
                    Some(read_exact(payload, &mut pos, bytes)?.to_vec())
                } else {
                    None
                };
                let bitmap = nulls.as_deref();
                let col = match col_type {
                    ColumnType::F64 => {
                        let mut values = Vec::with_capacity(row_count);
                        for idx in 0..row_count {
                            let v = f64::from_le_bytes(
                                read_exact(payload, &mut pos, 8)?.try_into().unwrap(),
                            );
                            if bitmap_is_set(bitmap, idx) {
                                values.push(Some(v));
                            } else {
                                values.push(None);
                            }
                        }
                        ColumnData::F64(values)
                    }
                    ColumnType::I64 => {
                        let mut values = Vec::with_capacity(row_count);
                        for idx in 0..row_count {
                            let v = i64::from_le_bytes(
                                read_exact(payload, &mut pos, 8)?.try_into().unwrap(),
                            );
                            if bitmap_is_set(bitmap, idx) {
                                values.push(Some(v));
                            } else {
                                values.push(None);
                            }
                        }
                        ColumnData::I64(values)
                    }
                    ColumnType::Bool => {
                        let mut values = Vec::with_capacity(row_count);
                        for idx in 0..row_count {
                            let v = read_exact(payload, &mut pos, 1)?[0] != 0;
                            if bitmap_is_set(bitmap, idx) {
                                values.push(Some(v));
                            } else {
                                values.push(None);
                            }
                        }
                        ColumnData::Bool(values)
                    }
                    ColumnType::Utf8 => {
                        let mut values = Vec::with_capacity(row_count);
                        for idx in 0..row_count {
                            let len = u32::from_le_bytes(
                                read_exact(payload, &mut pos, 4)?.try_into().unwrap(),
                            ) as usize;
                            let bytes = read_exact(payload, &mut pos, len)?;
                            if bitmap_is_set(bitmap, idx) {
                                let s = std::str::from_utf8(bytes).map_err(|_| {
                                    io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8")
                                })?;
                                values.push(Some(s.to_string()));
                            } else {
                                values.push(None);
                            }
                        }
                        ColumnData::Utf8(values)
                    }
                };
                cols.push(col);
            }
            let table = std::str::from_utf8(table_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid table utf-8")
            })?;
            Ok(WalRecordOwned::AppendBatch {
                seq_start,
                table: table.to_string(),
                ts_ms,
                cols,
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown WAL record",
        )),
    }?;

    if pos != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in WAL payload",
        ));
    }
    Ok(record)
}

fn encode_null_bitmap<T>(values: &[Option<T>], payload: &mut Vec<u8>) -> io::Result<()> {
    if values.iter().any(|v| v.is_none()) {
        payload.push(1);
        let bytes = (values.len() + 7) / 8;
        let mut bitmap = vec![0u8; bytes];
        for (idx, v) in values.iter().enumerate() {
            if v.is_some() {
                let byte = idx / 8;
                let bit = idx % 8;
                bitmap[byte] |= 1u8 << bit;
            }
        }
        payload.extend_from_slice(&bitmap);
    } else {
        payload.push(0);
    }
    Ok(())
}

fn bitmap_is_set(bitmap: Option<&[u8]>, idx: usize) -> bool {
    match bitmap {
        Some(bitmap) => {
            let byte = idx / 8;
            let bit = idx % 8;
            if byte >= bitmap.len() {
                return false;
            }
            (bitmap[byte] & (1u8 << bit)) != 0
        }
        None => true,
    }
}
