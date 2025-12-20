#![no_main]

use libfuzzer_sys::fuzz_target;
use nanots::c_api;
use std::ffi::CString;
use std::fs;
use std::path::PathBuf;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let mut path = std::env::temp_dir();
    path.push(temp_name(data));
    let path_c = match CString::new(path.to_string_lossy().as_bytes()) {
        Ok(s) => s,
        Err(_) => return,
    };

    let retention_ms = if data.len() > 1 {
        data[1] as i64 * 1000
    } else {
        -1
    };
    let handle = unsafe { c_api::nanots_open(path_c.as_ptr(), retention_ms) };
    if handle.is_null() {
        let _ = fs::remove_file(&path);
        return;
    }

    let table = CString::new("t").unwrap();
    let col = CString::new("value").unwrap();
    let cols = [col.as_ptr()];
    unsafe {
        let _ = c_api::nanots_create_table(handle, table.as_ptr(), cols.as_ptr(), cols.len());
    }

    let mut i = 2usize;
    let mut seq = 0i64;
    while i + 8 <= data.len() && seq < 64 {
        let ts = 1_700_000_000_000i64 + seq * 1000;
        let raw = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
        let value = f64::from_bits(raw);
        unsafe {
            let _ = c_api::nanots_append(handle, table.as_ptr(), ts, value);
        }
        i += 8;
        seq += 1;
    }

    unsafe {
        let _ = c_api::nanots_flush(handle);
    }

    let mut out_len: usize = 0;
    unsafe {
        let ptr = c_api::nanots_query_range(handle, table.as_ptr(), 0, i64::MAX, &mut out_len);
        if !ptr.is_null() {
            c_api::nanots_query_free(ptr, out_len);
        }
    }

    unsafe {
        c_api::nanots_close(handle);
    }
    let _ = fs::remove_file(&path);
});

fn temp_name(data: &[u8]) -> PathBuf {
    let mut hash = 0u64;
    for &b in data.iter().take(1024) {
        hash = hash.wrapping_mul(131).wrapping_add(b as u64);
    }
    PathBuf::from(format!("nanots_fuzz_c_api_{}.ntt", hash))
}
