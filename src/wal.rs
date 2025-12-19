// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::dbfile;
use std::io;
use std::path::{Path, PathBuf};

const WAL_MAGIC: &[u8; 4] = b"NTWL";
const WAL_VERSION: u8 = 1;
const RECORD_APPEND: u8 = 1;
const RECORD_APPEND_ROW: u8 = 2;

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
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown WAL record",
        )),
    }
}
