#![no_main]

use libfuzzer_sys::fuzz_target;
use nanots::dbfile;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let mut path = std::env::temp_dir();
    path.push(temp_name(data));

    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)
    {
        let _ = file.write_all(data);
        let _ = file.flush();
        let _ = dbfile::iter_records(&path, |_, _| Ok(()));
    }
    let _ = fs::remove_file(&path);
});

fn temp_name(data: &[u8]) -> PathBuf {
    let mut hash = 0u64;
    for &b in data.iter().take(1024) {
        hash = hash.wrapping_mul(131).wrapping_add(b as u64);
    }
    PathBuf::from(format!("nanots_fuzz_dbfile_{}.ntt", hash))
}
