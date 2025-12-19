// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::{NanoTsDb, NanoTsOptions, Point};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_longlong};
use std::ptr;
use std::sync::Mutex;
use std::time::Duration;

#[cfg(feature = "arrow")]
use crate::arrow::{ArrowArray, ArrowSchema};

#[repr(C)]
pub struct NanotsPoint {
    pub ts_ms: i64,
    pub value: f64,
}

pub struct NanotsHandle {
    db: Mutex<NanoTsDb>,
}

fn cstr_to_string(s: *const c_char) -> Result<String, c_int> {
    if s.is_null() {
        return Err(-2);
    }
    unsafe { CStr::from_ptr(s) }
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| -3)
}

#[no_mangle]
pub extern "C" fn nanots_open(path: *const c_char, retention_ms: c_longlong) -> *mut NanotsHandle {
    let Ok(path) = cstr_to_string(path) else {
        return ptr::null_mut();
    };
    let opts = NanoTsOptions {
        retention: if retention_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(retention_ms as u64))
        },
        ..Default::default()
    };
    match NanoTsDb::open(path, opts) {
        Ok(db) => Box::into_raw(Box::new(NanotsHandle { db: Mutex::new(db) })),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn nanots_close(handle: *mut NanotsHandle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[no_mangle]
pub extern "C" fn nanots_append(
    handle: *mut NanotsHandle,
    series: *const c_char,
    ts_ms: c_longlong,
    value: f64,
) -> c_longlong {
    if handle.is_null() {
        return -2;
    }
    let Ok(series) = cstr_to_string(series) else {
        return -3;
    };
    let handle = unsafe { &*handle };
    let Ok(mut db) = handle.db.lock() else {
        return -4;
    };
    match db.append(&series, ts_ms as i64, value) {
        Ok(seq) => seq as c_longlong,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn nanots_flush(handle: *mut NanotsHandle) -> c_int {
    if handle.is_null() {
        return -2;
    }
    let handle = unsafe { &*handle };
    let Ok(mut db) = handle.db.lock() else {
        return -4;
    };
    match db.flush() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn nanots_create_table(
    handle: *mut NanotsHandle,
    table: *const c_char,
    columns: *const *const c_char,
    ncols: usize,
) -> c_int {
    if handle.is_null() {
        return -2;
    }
    let Ok(table) = cstr_to_string(table) else {
        return -3;
    };
    if columns.is_null() && ncols != 0 {
        return -2;
    }
    let mut col_vec: Vec<String> = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let p = unsafe { *columns.add(i) };
        let Ok(s) = cstr_to_string(p) else {
            return -3;
        };
        col_vec.push(s);
    }
    let col_refs: Vec<&str> = col_vec.iter().map(|s| s.as_str()).collect();

    let handle = unsafe { &*handle };
    let Ok(mut db) = handle.db.lock() else {
        return -4;
    };
    match db.create_table(&table, &col_refs) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn nanots_append_row(
    handle: *mut NanotsHandle,
    table: *const c_char,
    ts_ms: c_longlong,
    values: *const f64,
    nvals: usize,
) -> c_longlong {
    if handle.is_null() {
        return -2;
    }
    let Ok(table) = cstr_to_string(table) else {
        return -3;
    };
    if values.is_null() && nvals != 0 {
        return -2;
    }
    let slice = unsafe { std::slice::from_raw_parts(values, nvals) };

    let handle = unsafe { &*handle };
    let Ok(mut db) = handle.db.lock() else {
        return -4;
    };
    match db.append_row(&table, ts_ms as i64, slice) {
        Ok(seq) => seq as c_longlong,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn nanots_query_range(
    handle: *mut NanotsHandle,
    series: *const c_char,
    start_ms: c_longlong,
    end_ms: c_longlong,
    out_len: *mut usize,
) -> *mut NanotsPoint {
    if handle.is_null() || out_len.is_null() {
        return ptr::null_mut();
    }
    let Ok(series) = cstr_to_string(series) else {
        unsafe { *out_len = 0 };
        return ptr::null_mut();
    };
    let handle = unsafe { &*handle };
    let Ok(db) = handle.db.lock() else {
        unsafe { *out_len = 0 };
        return ptr::null_mut();
    };

    let Ok(points) = db.query_range(&series, start_ms as i64, end_ms as i64) else {
        unsafe { *out_len = 0 };
        return ptr::null_mut();
    };

    let mut out: Vec<NanotsPoint> = Vec::with_capacity(points.len());
    for p in points {
        out.push(NanotsPoint {
            ts_ms: p.ts_ms,
            value: p.value,
        });
    }
    let len = out.len();
    let ptr = out.as_mut_ptr();
    std::mem::forget(out);
    unsafe { *out_len = len };
    ptr
}

#[no_mangle]
pub extern "C" fn nanots_query_free(ptr: *mut NanotsPoint, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    unsafe {
        drop(Vec::from_raw_parts(ptr, len, len));
    }
}

#[no_mangle]
pub extern "C" fn nanots_point_size() -> usize {
    std::mem::size_of::<NanotsPoint>()
}

impl From<Point> for NanotsPoint {
    fn from(p: Point) -> Self {
        Self {
            ts_ms: p.ts_ms,
            value: p.value,
        }
    }
}

#[cfg(feature = "arrow")]
#[no_mangle]
pub extern "C" fn nanots_query_table_range_arrow(
    handle: *mut NanotsHandle,
    table: *const c_char,
    start_ms: c_longlong,
    end_ms: c_longlong,
    out_array: *mut ArrowArray,
    out_schema: *mut ArrowSchema,
) -> c_int {
    if handle.is_null() || out_array.is_null() || out_schema.is_null() {
        return -2;
    }
    let Ok(table) = cstr_to_string(table) else {
        return -3;
    };
    let handle = unsafe { &*handle };
    let Ok(db) = handle.db.lock() else {
        return -4;
    };

    let Ok((ts, cols)) = db.query_table_range_columns(&table, start_ms as i64, end_ms as i64) else {
        return -1;
    };
    let Ok(schema) = db.table_schema(&table) else {
        return -1;
    };
    if cols.len() != schema.columns.len() {
        return -1;
    }

    let mut columns: Vec<(&str, Vec<f64>)> = Vec::with_capacity(schema.columns.len());
    for (name, col) in schema.columns.iter().zip(cols) {
        columns.push((name.as_str(), col));
    }

    match crate::arrow::export_ts_f64_table_to_c(&table, ts, columns, unsafe { &mut *out_array }, unsafe { &mut *out_schema }) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}
