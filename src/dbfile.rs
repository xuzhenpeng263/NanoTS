// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub const FILE_MAGIC: &[u8; 4] = b"NTSF";
pub const FILE_VERSION: u8 = 1;

pub const RECORD_META: u8 = 1;
pub const RECORD_SCHEMA: u8 = 2;
pub const RECORD_TABLE_SEGMENT: u8 = 3;
pub const RECORD_WAL: u8 = 4;
pub const RECORD_WAL_CHECKPOINT: u8 = 5;
pub const RECORD_SERIES_SEGMENT: u8 = 6;
pub const RECORD_TABLE_INDEX: u8 = 7;

const DEFAULT_MAX_RECORD_SIZE: u32 = 64 * 1024 * 1024;

fn max_record_size() -> u32 {
    static LIMIT: OnceLock<u32> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("NANOTS_MAX_RECORD_SIZE")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MAX_RECORD_SIZE)
    })
}

#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub record_type: u8,
    pub payload_len: u32,
    pub record_offset: u64,
    pub payload_offset: u64,
}

pub(crate) fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c9dc5;
    for &b in bytes {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

pub fn ensure_db_file(path: &Path) -> io::Result<()> {
    if path.exists() && path.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "single-file mode requires a file path, not a directory",
        ));
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        file.write_all(FILE_MAGIC)?;
        file.write_all(&[FILE_VERSION])?;
        file.flush()?;
        return Ok(());
    }
    if len < 5 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "db file header too short",
        ));
    }
    let mut hdr = [0u8; 5];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut hdr)?;
    if &hdr[0..4] != FILE_MAGIC || hdr[4] != FILE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid db file header",
        ));
    }
    Ok(())
}

pub fn create_new_db_file(path: &Path) -> io::Result<()> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let mut file = File::create(path)?;
    file.write_all(FILE_MAGIC)?;
    file.write_all(&[FILE_VERSION])?;
    file.flush()?;
    Ok(())
}

pub fn append_record(path: &Path, record_type: u8, payload: &[u8]) -> io::Result<u64> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    let record_offset = file.seek(SeekFrom::End(0))?;
    if payload.len() > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "record payload too large",
        ));
    }
    let payload_len = payload.len() as u32;
    let mut header = [0u8; 5];
    header[0] = record_type;
    header[1..5].copy_from_slice(&payload_len.to_le_bytes());

    let mut checksum_buf = Vec::with_capacity(1 + payload.len());
    checksum_buf.push(record_type);
    checksum_buf.extend_from_slice(&payload_len.to_le_bytes());
    checksum_buf.extend_from_slice(payload);
    let checksum = fnv1a32(&checksum_buf);

    let mut writer = BufWriter::new(&mut file);
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    writer.write_all(&checksum.to_le_bytes())?;
    writer.flush()?;
    Ok(record_offset)
}

pub fn iter_records<F>(path: &Path, mut on_record: F) -> io::Result<()>
where
    F: FnMut(RecordHeader, &[u8]) -> io::Result<()>,
{
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut hdr = [0u8; 5];
    if reader.read_exact(&mut hdr).is_err() {
        return Ok(());
    }
    if &hdr[0..4] != FILE_MAGIC || hdr[4] != FILE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid db file header",
        ));
    }
    let mut offset = 5u64;
    loop {
        let mut head = [0u8; 5];
        match reader.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        let record_type = head[0];
        let payload_len = u32::from_le_bytes(head[1..5].try_into().unwrap());
        if payload_len > max_record_size() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record payload too large",
            ));
        }
        let payload_offset = offset + 5;
        offset += 5;
        let remaining = file_len.saturating_sub(offset);
        let needed = payload_len as u64 + 4;
        if remaining < needed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record length out of bounds",
            ));
        }

        let mut payload = vec![0u8; payload_len as usize];
        reader.read_exact(&mut payload)?;
        offset += payload_len as u64;

        let mut checksum_buf = [0u8; 4];
        reader.read_exact(&mut checksum_buf)?;
        offset += 4;
        let checksum = u32::from_le_bytes(checksum_buf);

        let mut verify = Vec::with_capacity(1 + payload.len());
        verify.push(record_type);
        verify.extend_from_slice(&payload_len.to_le_bytes());
        verify.extend_from_slice(&payload);
        if fnv1a32(&verify) != checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record checksum mismatch",
            ));
        }

        let header = RecordHeader {
            record_type,
            payload_len,
            record_offset: payload_offset - 5,
            payload_offset,
        };
        on_record(header, &payload)?;
    }
}

pub fn temp_db_path(base: &Path, suffix: &str) -> PathBuf {
    let mut out = PathBuf::from(base);
    let stamp = format!(
        "{}.{}.{}.tmp",
        suffix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    if let Some(name) = base.file_name() {
        out.set_file_name(format!("{}.{}", name.to_string_lossy(), stamp));
    } else {
        out.set_file_name(stamp);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nanots_dbfile_test_{}_{}_{}.ntt",
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
    fn test_iter_records_truncated_checksum_is_error() {
        let path = temp_path("trunc_checksum");
        let mut file = File::create(&path).unwrap();
        file.write_all(FILE_MAGIC).unwrap();
        file.write_all(&[FILE_VERSION]).unwrap();

        let payload_len: u32 = 4;
        let mut header = [0u8; 5];
        header[0] = RECORD_META;
        header[1..5].copy_from_slice(&payload_len.to_le_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&[1, 2, 3, 4]).unwrap();
        file.flush().unwrap();

        let err = iter_records(&path, |_hdr, _payload| Ok(())).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_file(path);
    }
}
