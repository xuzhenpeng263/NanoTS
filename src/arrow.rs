// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Apache Arrow C Data Interface producer.
// Exports a "record batch" as a StructArray with children columns.
//
// Supported types:
// - `ts_ms`: Int64 (format "l")
// - value columns: Float64/Int64/Bool/Utf8 (formats "g"/"l"/"b"/"u")

use crate::db::ColumnData;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CString};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(C)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

#[repr(C)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

#[derive(Debug)]
pub struct ArrowExportError;

pub struct ArrowBatch {
    array: ArrowArray,
    schema: ArrowSchema,
}

impl ArrowBatch {
    pub fn from_ts_columns(
        root_name: &str,
        ts_ms: Vec<i64>,
        columns: Vec<(&str, ColumnData)>,
    ) -> Result<Self, ArrowExportError> {
        let mut array = ArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: ptr::null_mut(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        };
        let mut schema = ArrowSchema {
            format: ptr::null(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        };

        export_ts_table_to_c(root_name, ts_ms, columns, &mut array, &mut schema)?;
        Ok(Self { array, schema })
    }

    pub fn from_ts_f64_columns(
        root_name: &str,
        ts_ms: Vec<i64>,
        columns: Vec<(&str, Vec<f64>)>,
    ) -> Result<Self, ArrowExportError> {
        let cols = columns
            .into_iter()
            .map(|(name, col)| {
                let data = col.into_iter().map(Some).collect();
                (name, ColumnData::F64(data))
            })
            .collect();
        Self::from_ts_columns(root_name, ts_ms, cols)
    }

    pub fn array_ptr(&mut self) -> *mut ArrowArray {
        &mut self.array
    }

    pub fn schema_ptr(&mut self) -> *mut ArrowSchema {
        &mut self.schema
    }
}

impl Drop for ArrowBatch {
    fn drop(&mut self) {
        if let Some(release) = self.array.release {
            release(&mut self.array);
        }
        if let Some(release) = self.schema.release {
            release(&mut self.schema);
        }
    }
}

struct PrivateData {
    refcnt: AtomicUsize,

    // Data
    ts: Vec<i64>,
    cols: Vec<ColumnBufferData>,

    // CStrings for schema
    root_format: CString,
    ts_format: CString,
    f64_format: CString,
    i64_format: CString,
    bool_format: CString,
    utf8_format: CString,
    root_name: CString,
    field_names: Vec<CString>,

    // Arrays & schemas (owned so pointers stay valid)
    child_arrays: Vec<Box<ArrowArray>>,
    child_schemas: Vec<Box<ArrowSchema>>,

    // Pointer arrays referenced by ArrowArray/ArrowSchema
    root_buffers: Vec<*const c_void>,
    child_buffers: Vec<Vec<*const c_void>>,
    array_children_ptrs: Vec<*mut ArrowArray>,
    schema_children_ptrs: Vec<*mut ArrowSchema>,
}

enum ColumnBufferData {
    F64 {
        values: Vec<f64>,
        nulls: Option<Vec<u8>>,
        null_count: i64,
    },
    I64 {
        values: Vec<i64>,
        nulls: Option<Vec<u8>>,
        null_count: i64,
    },
    Bool {
        values: Vec<u8>,
        nulls: Option<Vec<u8>>,
        null_count: i64,
    },
    Utf8 {
        offsets: Vec<i32>,
        data: Vec<u8>,
        nulls: Option<Vec<u8>>,
        null_count: i64,
    },
}

extern "C" fn release_noop_array(array: *mut ArrowArray) {
    if array.is_null() {
        return;
    }
    unsafe {
        let a = &mut *array;
        a.release = None;
    }
}

extern "C" fn release_noop_schema(schema: *mut ArrowSchema) {
    if schema.is_null() {
        return;
    }
    unsafe {
        let s = &mut *schema;
        s.release = None;
    }
}

fn build_validity_bitmap<T>(values: &[Option<T>]) -> (Option<Vec<u8>>, i64) {
    if values.is_empty() {
        return (None, 0);
    }
    let mut any_null = false;
    let mut null_count = 0i64;
    let mut out = vec![0u8; (values.len() + 7) / 8];
    for (i, v) in values.iter().enumerate() {
        if v.is_some() {
            let byte = i / 8;
            let bit = i % 8;
            out[byte] |= 1u8 << bit;
        } else {
            any_null = true;
            null_count += 1;
        }
    }
    if any_null {
        (Some(out), null_count)
    } else {
        (None, 0)
    }
}

fn build_bool_buffers(values: &[Option<bool>]) -> (Vec<u8>, Option<Vec<u8>>, i64) {
    let (nulls, null_count) = build_validity_bitmap(values);
    let mut out = vec![0u8; (values.len() + 7) / 8];
    for (i, v) in values.iter().enumerate() {
        if v.unwrap_or(false) {
            let byte = i / 8;
            let bit = i % 8;
            out[byte] |= 1u8 << bit;
        }
    }
    (out, nulls, null_count)
}

impl PrivateData {
    fn new(
        root_name: &str,
        ts: Vec<i64>,
        columns: Vec<(&str, ColumnData)>,
    ) -> Result<Self, ArrowExportError> {
        let mut field_names: Vec<CString> = Vec::with_capacity(1 + columns.len());
        field_names.push(CString::new("ts_ms").map_err(|_| ArrowExportError)?);
        for (name, _) in &columns {
            field_names.push(CString::new(*name).map_err(|_| ArrowExportError)?);
        }

        let n_rows = ts.len();
        let mut cols: Vec<ColumnBufferData> = Vec::with_capacity(columns.len());
        for (_, col) in columns {
            let col = match col {
                ColumnData::F64(values) => {
                    if values.len() != n_rows {
                        return Err(ArrowExportError);
                    }
                    let (nulls, null_count) = build_validity_bitmap(&values);
                    let raw: Vec<f64> = values.iter().map(|v| v.unwrap_or(0.0)).collect();
                    ColumnBufferData::F64 {
                        values: raw,
                        nulls,
                        null_count,
                    }
                }
                ColumnData::I64(values) => {
                    if values.len() != n_rows {
                        return Err(ArrowExportError);
                    }
                    let (nulls, null_count) = build_validity_bitmap(&values);
                    let raw: Vec<i64> = values.iter().map(|v| v.unwrap_or(0)).collect();
                    ColumnBufferData::I64 {
                        values: raw,
                        nulls,
                        null_count,
                    }
                }
                ColumnData::Bool(values) => {
                    if values.len() != n_rows {
                        return Err(ArrowExportError);
                    }
                    let (values, nulls, null_count) = build_bool_buffers(&values);
                    ColumnBufferData::Bool {
                        values,
                        nulls,
                        null_count,
                    }
                }
                ColumnData::Utf8(values) => {
                    if values.len() != n_rows {
                        return Err(ArrowExportError);
                    }
                    let (nulls, null_count) = build_validity_bitmap(&values);
                    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
                    let mut data: Vec<u8> = Vec::new();
                    offsets.push(0);
                    for v in values {
                        if let Some(s) = v {
                            data.extend_from_slice(s.as_bytes());
                        }
                        if data.len() > i32::MAX as usize {
                            return Err(ArrowExportError);
                        }
                        offsets.push(data.len() as i32);
                    }
                    ColumnBufferData::Utf8 {
                        offsets,
                        data,
                        nulls,
                        null_count,
                    }
                }
            };
            cols.push(col);
        }

        Ok(Self {
            refcnt: AtomicUsize::new(0),
            ts,
            cols,
            root_format: CString::new("+s").map_err(|_| ArrowExportError)?,
            ts_format: CString::new("l").map_err(|_| ArrowExportError)?,
            f64_format: CString::new("g").map_err(|_| ArrowExportError)?,
            i64_format: CString::new("l").map_err(|_| ArrowExportError)?,
            bool_format: CString::new("b").map_err(|_| ArrowExportError)?,
            utf8_format: CString::new("u").map_err(|_| ArrowExportError)?,
            root_name: CString::new(root_name).map_err(|_| ArrowExportError)?,
            field_names,
            child_arrays: Vec::new(),
            child_schemas: Vec::new(),
            root_buffers: vec![ptr::null()],
            child_buffers: Vec::new(),
            array_children_ptrs: Vec::new(),
            schema_children_ptrs: Vec::new(),
        })
    }

    fn build(&mut self, out_array: *mut ArrowArray, out_schema: *mut ArrowSchema) -> Result<(), ArrowExportError> {
        if out_array.is_null() || out_schema.is_null() {
            return Err(ArrowExportError);
        }

        let n_rows = self.ts.len();

        let n_children = 1 + self.cols.len();

        self.child_arrays.clear();
        self.child_schemas.clear();
        self.child_buffers.clear();
        self.array_children_ptrs.clear();
        self.schema_children_ptrs.clear();

        self.child_arrays.reserve(n_children);
        self.child_schemas.reserve(n_children);
        self.child_buffers.reserve(n_children);

        // Child 0: ts_ms (Int64)
        self.child_buffers.push(vec![ptr::null(), self.ts.as_ptr() as *const c_void]);
        self.child_arrays.push(Box::new(ArrowArray {
            length: n_rows as i64,
            null_count: 0,
            offset: 0,
            n_buffers: 2,
            n_children: 0,
            buffers: self.child_buffers[0].as_mut_ptr(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: Some(release_noop_array),
            private_data: ptr::null_mut(),
        }));
        self.child_schemas.push(Box::new(ArrowSchema {
            format: self.ts_format.as_ptr(),
            name: self.field_names[0].as_ptr(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: Some(release_noop_schema),
            private_data: ptr::null_mut(),
        }));

        // Children 1..: value columns
        for (idx, col) in self.cols.iter().enumerate() {
            let child_idx = 1 + idx;
            match col {
                ColumnBufferData::F64 {
                    values,
                    nulls,
                    null_count,
                } => {
                    let null_ptr = nulls
                        .as_ref()
                        .map(|v| v.as_ptr() as *const c_void)
                        .unwrap_or(ptr::null());
                    self.child_buffers
                        .push(vec![null_ptr, values.as_ptr() as *const c_void]);
                    self.child_arrays.push(Box::new(ArrowArray {
                        length: n_rows as i64,
                        null_count: *null_count,
                        offset: 0,
                        n_buffers: 2,
                        n_children: 0,
                        buffers: self.child_buffers[child_idx].as_mut_ptr(),
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_array),
                        private_data: ptr::null_mut(),
                    }));
                    let flags = if *null_count > 0 { 2 } else { 0 };
                    self.child_schemas.push(Box::new(ArrowSchema {
                        format: self.f64_format.as_ptr(),
                        name: self.field_names[child_idx].as_ptr(),
                        metadata: ptr::null(),
                        flags,
                        n_children: 0,
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_schema),
                        private_data: ptr::null_mut(),
                    }));
                }
                ColumnBufferData::I64 {
                    values,
                    nulls,
                    null_count,
                } => {
                    let null_ptr = nulls
                        .as_ref()
                        .map(|v| v.as_ptr() as *const c_void)
                        .unwrap_or(ptr::null());
                    self.child_buffers
                        .push(vec![null_ptr, values.as_ptr() as *const c_void]);
                    self.child_arrays.push(Box::new(ArrowArray {
                        length: n_rows as i64,
                        null_count: *null_count,
                        offset: 0,
                        n_buffers: 2,
                        n_children: 0,
                        buffers: self.child_buffers[child_idx].as_mut_ptr(),
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_array),
                        private_data: ptr::null_mut(),
                    }));
                    let flags = if *null_count > 0 { 2 } else { 0 };
                    self.child_schemas.push(Box::new(ArrowSchema {
                        format: self.i64_format.as_ptr(),
                        name: self.field_names[child_idx].as_ptr(),
                        metadata: ptr::null(),
                        flags,
                        n_children: 0,
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_schema),
                        private_data: ptr::null_mut(),
                    }));
                }
                ColumnBufferData::Bool {
                    values,
                    nulls,
                    null_count,
                } => {
                    let null_ptr = nulls
                        .as_ref()
                        .map(|v| v.as_ptr() as *const c_void)
                        .unwrap_or(ptr::null());
                    self.child_buffers
                        .push(vec![null_ptr, values.as_ptr() as *const c_void]);
                    self.child_arrays.push(Box::new(ArrowArray {
                        length: n_rows as i64,
                        null_count: *null_count,
                        offset: 0,
                        n_buffers: 2,
                        n_children: 0,
                        buffers: self.child_buffers[child_idx].as_mut_ptr(),
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_array),
                        private_data: ptr::null_mut(),
                    }));
                    let flags = if *null_count > 0 { 2 } else { 0 };
                    self.child_schemas.push(Box::new(ArrowSchema {
                        format: self.bool_format.as_ptr(),
                        name: self.field_names[child_idx].as_ptr(),
                        metadata: ptr::null(),
                        flags,
                        n_children: 0,
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_schema),
                        private_data: ptr::null_mut(),
                    }));
                }
                ColumnBufferData::Utf8 {
                    offsets,
                    data,
                    nulls,
                    null_count,
                } => {
                    let null_ptr = nulls
                        .as_ref()
                        .map(|v| v.as_ptr() as *const c_void)
                        .unwrap_or(ptr::null());
                    self.child_buffers.push(vec![
                        null_ptr,
                        offsets.as_ptr() as *const c_void,
                        data.as_ptr() as *const c_void,
                    ]);
                    self.child_arrays.push(Box::new(ArrowArray {
                        length: n_rows as i64,
                        null_count: *null_count,
                        offset: 0,
                        n_buffers: 3,
                        n_children: 0,
                        buffers: self.child_buffers[child_idx].as_mut_ptr(),
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_array),
                        private_data: ptr::null_mut(),
                    }));
                    let flags = if *null_count > 0 { 2 } else { 0 };
                    self.child_schemas.push(Box::new(ArrowSchema {
                        format: self.utf8_format.as_ptr(),
                        name: self.field_names[child_idx].as_ptr(),
                        metadata: ptr::null(),
                        flags,
                        n_children: 0,
                        children: ptr::null_mut(),
                        dictionary: ptr::null_mut(),
                        release: Some(release_noop_schema),
                        private_data: ptr::null_mut(),
                    }));
                }
            }
        }

        // Build children pointers arrays.
        self.array_children_ptrs = self
            .child_arrays
            .iter_mut()
            .map(|a| &mut **a as *mut ArrowArray)
            .collect();
        self.schema_children_ptrs = self
            .child_schemas
            .iter_mut()
            .map(|s| &mut **s as *mut ArrowSchema)
            .collect();

        unsafe {
            *out_array = ArrowArray {
                length: n_rows as i64,
                null_count: 0,
                offset: 0,
                n_buffers: 1,
                n_children: n_children as i64,
                buffers: self.root_buffers.as_mut_ptr(),
                children: self.array_children_ptrs.as_mut_ptr(),
                dictionary: ptr::null_mut(),
                release: Some(release_array),
                private_data: ptr::null_mut(),
            };
            *out_schema = ArrowSchema {
                format: self.root_format.as_ptr(),
                name: self.root_name.as_ptr(),
                metadata: ptr::null(),
                flags: 0,
                n_children: n_children as i64,
                children: self.schema_children_ptrs.as_mut_ptr(),
                dictionary: ptr::null_mut(),
                release: Some(release_schema),
                private_data: ptr::null_mut(),
            };
        }

        Ok(())
    }
}

extern "C" fn release_array(array: *mut ArrowArray) {
    if array.is_null() {
        return;
    }
    unsafe {
        let a = &mut *array;
        if a.release.is_none() {
            return;
        }
        a.release = None;
        let private = a.private_data;
        a.private_data = ptr::null_mut();

        if !private.is_null() {
            let private = private as *mut PrivateData;
            if (*private).refcnt.fetch_sub(1, Ordering::AcqRel) == 1 {
                drop(Box::from_raw(private));
            }
        }
    }
}

extern "C" fn release_schema(schema: *mut ArrowSchema) {
    if schema.is_null() {
        return;
    }
    unsafe {
        let s = &mut *schema;
        if s.release.is_none() {
            return;
        }
        s.release = None;
        let private = s.private_data;
        s.private_data = ptr::null_mut();

        if !private.is_null() {
            let private = private as *mut PrivateData;
            if (*private).refcnt.fetch_sub(1, Ordering::AcqRel) == 1 {
                drop(Box::from_raw(private));
            }
        }
    }
}

/// Exports a table-like dataset as an Arrow StructArray via the C Data Interface.
///
/// - `root_name`: schema/root name for the struct
/// - `ts_ms`: timestamp column (required)
/// - `columns`: `[(name, values)]`, supported types are Float64/Int64/Bool/Utf8
///
/// On success, fills `out_array/out_schema`. The consumer must call `out_array.release(out_array)`
/// and `out_schema.release(out_schema)` exactly once.
pub fn export_ts_table_to_c(
    root_name: &str,
    ts_ms: Vec<i64>,
    columns: Vec<(&str, ColumnData)>,
    out_array: &mut ArrowArray,
    out_schema: &mut ArrowSchema,
) -> Result<(), ArrowExportError> {
    let mut private = Box::new(PrivateData::new(root_name, ts_ms, columns)?);
    private.build(out_array as *mut ArrowArray, out_schema as *mut ArrowSchema)?;

    // Private data is shared by array + schema. Free when both are released.
    private.refcnt.store(2, Ordering::Release);
    let private_ptr = Box::into_raw(private) as *mut c_void;
    out_array.private_data = private_ptr;
    out_schema.private_data = private_ptr;
    Ok(())
}

pub fn export_ts_f64_table_to_c(
    root_name: &str,
    ts_ms: Vec<i64>,
    columns: Vec<(&str, Vec<f64>)>,
    out_array: &mut ArrowArray,
    out_schema: &mut ArrowSchema,
) -> Result<(), ArrowExportError> {
    let cols = columns
        .into_iter()
        .map(|(name, col)| {
            let data = col.into_iter().map(Some).collect();
            (name, ColumnData::F64(data))
        })
        .collect();
    export_ts_table_to_c(root_name, ts_ms, cols, out_array, out_schema)
}

#[derive(Debug)]
pub enum ArrowImportError {
    NullPointer(&'static str),
    InvalidFormat(&'static str),
    InvalidData(&'static str),
    Utf8,
}

impl std::fmt::Display for ArrowImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowImportError::NullPointer(what) => write!(f, "null pointer: {}", what),
            ArrowImportError::InvalidFormat(what) => write!(f, "invalid format: {}", what),
            ArrowImportError::InvalidData(what) => write!(f, "invalid data: {}", what),
            ArrowImportError::Utf8 => write!(f, "invalid utf-8 data"),
        }
    }
}

impl std::error::Error for ArrowImportError {}

pub struct ArrowImportBatch {
    pub ts_ms: Vec<i64>,
    pub columns: HashMap<String, ColumnData>,
}

struct ArrowReleaseGuard {
    array: *mut ArrowArray,
    schema: *mut ArrowSchema,
}

impl Drop for ArrowReleaseGuard {
    fn drop(&mut self) {
        unsafe {
            if !self.array.is_null() {
                if let Some(release) = (*self.array).release {
                    release(self.array);
                }
            }
            if !self.schema.is_null() {
                if let Some(release) = (*self.schema).release {
                    release(self.schema);
                }
            }
        }
    }
}

fn cstr_to_string(ptr: *const c_char) -> Result<String, ArrowImportError> {
    if ptr.is_null() {
        return Err(ArrowImportError::NullPointer("schema format/name"));
    }
    unsafe {
        std::ffi::CStr::from_ptr(ptr)
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| ArrowImportError::Utf8)
    }
}

fn bitmap_is_set(bitmap: &[u8], idx: usize) -> bool {
    let byte = idx / 8;
    let bit = idx % 8;
    if byte >= bitmap.len() {
        return false;
    }
    (bitmap[byte] & (1u8 << bit)) != 0
}

fn array_buffers(array: &ArrowArray) -> Result<&[*const c_void], ArrowImportError> {
    if array.n_buffers < 0 {
        return Err(ArrowImportError::InvalidData("negative buffer count"));
    }
    if array.n_buffers == 0 {
        return Ok(&[]);
    }
    if array.buffers.is_null() {
        return Err(ArrowImportError::NullPointer("buffers"));
    }
    let len = array.n_buffers as usize;
    Ok(unsafe { std::slice::from_raw_parts(array.buffers, len) })
}

fn validity_bitmap<'a>(
    array: &ArrowArray,
    buffers: &'a [*const c_void],
    len: usize,
    offset: usize,
) -> Result<Option<&'a [u8]>, ArrowImportError> {
    if array.null_count == 0 {
        return Ok(None);
    }
    if buffers.is_empty() {
        return Err(ArrowImportError::InvalidData("missing validity buffer"));
    }
    let ptr = buffers[0] as *const u8;
    if ptr.is_null() {
        return Err(ArrowImportError::InvalidData("null validity buffer"));
    }
    let bytes = (offset + len + 7) / 8;
    Ok(Some(unsafe { std::slice::from_raw_parts(ptr, bytes) }))
}

fn read_i64_column(array: &ArrowArray) -> Result<Vec<Option<i64>>, ArrowImportError> {
    let len = array.length;
    if len < 0 || array.offset < 0 {
        return Err(ArrowImportError::InvalidData("negative length/offset"));
    }
    let len = len as usize;
    let offset = array.offset as usize;
    let buffers = array_buffers(array)?;
    if buffers.len() < 2 {
        return Err(ArrowImportError::InvalidData("missing data buffer"));
    }
    let data_ptr = buffers[1] as *const i64;
    if data_ptr.is_null() {
        return Err(ArrowImportError::InvalidData("null data buffer"));
    }
    let data = unsafe { std::slice::from_raw_parts(data_ptr, offset + len) };
    let validity = validity_bitmap(array, &buffers, len, offset)?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let idx = offset + i;
        if validity.map(|b| bitmap_is_set(b, idx)).unwrap_or(true) {
            out.push(Some(data[idx]));
        } else {
            out.push(None);
        }
    }
    Ok(out)
}

fn read_f64_column(array: &ArrowArray) -> Result<Vec<Option<f64>>, ArrowImportError> {
    let len = array.length;
    if len < 0 || array.offset < 0 {
        return Err(ArrowImportError::InvalidData("negative length/offset"));
    }
    let len = len as usize;
    let offset = array.offset as usize;
    let buffers = array_buffers(array)?;
    if buffers.len() < 2 {
        return Err(ArrowImportError::InvalidData("missing data buffer"));
    }
    let data_ptr = buffers[1] as *const f64;
    if data_ptr.is_null() {
        return Err(ArrowImportError::InvalidData("null data buffer"));
    }
    let data = unsafe { std::slice::from_raw_parts(data_ptr, offset + len) };
    let validity = validity_bitmap(array, &buffers, len, offset)?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let idx = offset + i;
        if validity.map(|b| bitmap_is_set(b, idx)).unwrap_or(true) {
            out.push(Some(data[idx]));
        } else {
            out.push(None);
        }
    }
    Ok(out)
}

fn read_bool_column(array: &ArrowArray) -> Result<Vec<Option<bool>>, ArrowImportError> {
    let len = array.length;
    if len < 0 || array.offset < 0 {
        return Err(ArrowImportError::InvalidData("negative length/offset"));
    }
    let len = len as usize;
    let offset = array.offset as usize;
    let buffers = array_buffers(array)?;
    if buffers.len() < 2 {
        return Err(ArrowImportError::InvalidData("missing data buffer"));
    }
    let data_ptr = buffers[1] as *const u8;
    if data_ptr.is_null() {
        return Err(ArrowImportError::InvalidData("null data buffer"));
    }
    let data_len = (offset + len + 7) / 8;
    let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
    let validity = validity_bitmap(array, &buffers, len, offset)?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let idx = offset + i;
        if validity.map(|b| bitmap_is_set(b, idx)).unwrap_or(true) {
            out.push(Some(bitmap_is_set(data, idx)));
        } else {
            out.push(None);
        }
    }
    Ok(out)
}

fn read_utf8_column(array: &ArrowArray) -> Result<Vec<Option<String>>, ArrowImportError> {
    let len = array.length;
    if len < 0 || array.offset < 0 {
        return Err(ArrowImportError::InvalidData("negative length/offset"));
    }
    let len = len as usize;
    let offset = array.offset as usize;
    let buffers = array_buffers(array)?;
    if buffers.len() < 3 {
        return Err(ArrowImportError::InvalidData("missing utf8 buffers"));
    }
    let offsets_ptr = buffers[1] as *const i32;
    let data_ptr = buffers[2] as *const u8;
    if offsets_ptr.is_null() || data_ptr.is_null() {
        return Err(ArrowImportError::InvalidData("null utf8 buffers"));
    }
    let offsets_len = offset + len + 1;
    let offsets = unsafe { std::slice::from_raw_parts(offsets_ptr, offsets_len) };
    let last = offsets[offset + len];
    if last < 0 {
        return Err(ArrowImportError::InvalidData("negative utf8 offset"));
    }
    let data = unsafe { std::slice::from_raw_parts(data_ptr, last as usize) };
    let validity = validity_bitmap(array, &buffers, len, offset)?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let idx = offset + i;
        if !validity.map(|b| bitmap_is_set(b, idx)).unwrap_or(true) {
            out.push(None);
            continue;
        }
        let start = offsets[idx];
        let end = offsets[idx + 1];
        if start < 0 || end < start {
            return Err(ArrowImportError::InvalidData("bad utf8 offsets"));
        }
        let start = start as usize;
        let end = end as usize;
        if end > data.len() {
            return Err(ArrowImportError::InvalidData("utf8 offset out of bounds"));
        }
        let s = std::str::from_utf8(&data[start..end]).map_err(|_| ArrowImportError::Utf8)?;
        out.push(Some(s.to_string()));
    }
    Ok(out)
}

/// Imports a table-like Arrow StructArray from the C Data Interface.
/// Consumes `array`/`schema` by calling their `release` callbacks on return.
pub fn import_ts_table_from_c(
    schema: *mut ArrowSchema,
    array: *mut ArrowArray,
) -> Result<ArrowImportBatch, ArrowImportError> {
    if schema.is_null() {
        return Err(ArrowImportError::NullPointer("schema"));
    }
    if array.is_null() {
        return Err(ArrowImportError::NullPointer("array"));
    }
    let _guard = ArrowReleaseGuard { array, schema };
    let schema_ref = unsafe { &*schema };
    let array_ref = unsafe { &*array };
    if array_ref.length < 0 || array_ref.offset < 0 {
        return Err(ArrowImportError::InvalidData("negative root length/offset"));
    }
    let format = cstr_to_string(schema_ref.format)?;
    if format != "+s" {
        return Err(ArrowImportError::InvalidFormat("root must be struct (+s)"));
    }
    if schema_ref.n_children < 0 || array_ref.n_children < 0 {
        return Err(ArrowImportError::InvalidData("negative child count"));
    }
    if schema_ref.n_children != array_ref.n_children {
        return Err(ArrowImportError::InvalidData("child count mismatch"));
    }
    if array_ref.null_count != 0 {
        return Err(ArrowImportError::InvalidData("root struct contains nulls"));
    }
    let n_children = schema_ref.n_children as usize;
    if n_children == 0 {
        return Err(ArrowImportError::InvalidData("empty struct"));
    }
    if schema_ref.children.is_null() || array_ref.children.is_null() {
        return Err(ArrowImportError::NullPointer("children"));
    }
    let schema_children = unsafe { std::slice::from_raw_parts(schema_ref.children, n_children) };
    let array_children = unsafe { std::slice::from_raw_parts(array_ref.children, n_children) };
    let mut ts_ms: Option<Vec<i64>> = None;
    let mut columns: HashMap<String, ColumnData> = HashMap::new();
    for (child_schema_ptr, child_array_ptr) in
        schema_children.iter().zip(array_children.iter())
    {
        if child_schema_ptr.is_null() || child_array_ptr.is_null() {
            return Err(ArrowImportError::NullPointer("child"));
        }
        let child_schema = unsafe { &**child_schema_ptr };
        let child_array = unsafe { &**child_array_ptr };
        let name = cstr_to_string(child_schema.name)?;
        let child_format = cstr_to_string(child_schema.format)?;
        let len = child_array.length;
        if len < 0 {
            return Err(ArrowImportError::InvalidData("negative child length"));
        }
        if (len as usize) != array_ref.length as usize {
            return Err(ArrowImportError::InvalidData("child length mismatch"));
        }
        match child_format.as_str() {
            "l" => {
                let values = read_i64_column(child_array)?;
                if name == "ts_ms" || name == "ts" {
                    if values.iter().any(|v| v.is_none()) {
                        return Err(ArrowImportError::InvalidData("ts_ms contains nulls"));
                    }
                    ts_ms = Some(values.into_iter().map(|v| v.unwrap()).collect());
                } else {
                    columns.insert(name, ColumnData::I64(values));
                }
            }
            "g" => {
                let values = read_f64_column(child_array)?;
                columns.insert(name, ColumnData::F64(values));
            }
            "b" => {
                let values = read_bool_column(child_array)?;
                columns.insert(name, ColumnData::Bool(values));
            }
            "u" => {
                let values = read_utf8_column(child_array)?;
                columns.insert(name, ColumnData::Utf8(values));
            }
            _ => {
                return Err(ArrowImportError::InvalidFormat(
                    "unsupported child format",
                ));
            }
        }
    }
    let ts_ms = ts_ms.ok_or(ArrowImportError::InvalidData("missing ts_ms"))?;
    for col in columns.values() {
        let len = match col {
            ColumnData::F64(v) => v.len(),
            ColumnData::I64(v) => v.len(),
            ColumnData::Bool(v) => v.len(),
            ColumnData::Utf8(v) => v.len(),
        };
        if len != ts_ms.len() {
            return Err(ArrowImportError::InvalidData("column length mismatch"));
        }
    }
    Ok(ArrowImportBatch { ts_ms, columns })
}
