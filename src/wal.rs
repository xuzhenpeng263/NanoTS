// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::dbfile;
use std::io;
use std::path::{Path, PathBuf};

const WAL_MAGIC: &[u8; 4] = b"NTWL";
const WAL_VERSION: u8 = 1;
const RECORD_APPEND: u8 = 1;
const RECORD_APPEND_ROW: u8 = 2;
const RECORD_APPEND_ROW_TYPED: u8 = 3;
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
        values: Vec<crate::db::Value>,
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
        }

        let _ = dbfile::append_record(&self.db_path, dbfile::RECORD_WAL, &payload)?;
        Ok(())
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
    let mut pos = 5usize;
    let record_type = payload[pos];
    pos += 1;
    match record_type {
        RECORD_APPEND => {
            let seq = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let series_len = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let series_bytes = &payload[pos..pos + series_len];
            pos += series_len;
            let ts_ms = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let value = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());

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
            let seq = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let table_len = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let table_bytes = &payload[pos..pos + table_len];
            pos += table_len;
            let ts_ms = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let ncols = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            let need = ncols
                .checked_mul(8)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "overflow"))?;
            if payload.len() < pos + need {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "short WAL row payload",
                ));
            }
            let mut values: Vec<f64> = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let v = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                pos += 8;
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
            let seq = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let table_len = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let table_bytes = &payload[pos..pos + table_len];
            pos += table_len;
            let ts_ms = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let ncols = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            let mut values: Vec<crate::db::Value> = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                if payload.len() <= pos {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "short typed WAL payload",
                    ));
                }
                let tag = payload[pos];
                pos += 1;
                match tag {
                    WAL_VAL_NULL => values.push(crate::db::Value::Null),
                    WAL_VAL_F64 => {
                        if payload.len() < pos + 8 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "short typed WAL payload",
                            ));
                        }
                        let v = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                        pos += 8;
                        values.push(crate::db::Value::F64(v));
                    }
                    WAL_VAL_I64 => {
                        if payload.len() < pos + 8 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "short typed WAL payload",
                            ));
                        }
                        let v = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                        pos += 8;
                        values.push(crate::db::Value::I64(v));
                    }
                    WAL_VAL_BOOL => {
                        if payload.len() < pos + 1 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "short typed WAL payload",
                            ));
                        }
                        let v = payload[pos] != 0;
                        pos += 1;
                        values.push(crate::db::Value::Bool(v));
                    }
                    WAL_VAL_UTF8 => {
                        if payload.len() < pos + 4 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "short typed WAL payload",
                            ));
                        }
                        let len =
                            u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
                        pos += 4;
                        if payload.len() < pos + len {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "short typed WAL payload",
                            ));
                        }
                        let s = std::str::from_utf8(&payload[pos..pos + len]).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8")
                        })?;
                        pos += len;
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
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown WAL record",
        )),
    }
}
