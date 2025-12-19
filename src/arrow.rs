// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Apache Arrow C Data Interface producer.
// Exports a "record batch" as a StructArray with children columns.
//
// Supported types (v0):
// - `ts_ms`: Int64 (format "l")
// - value columns: Float64 (format "g")

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

pub struct ArrowExportError;

pub struct ArrowBatch {
    array: ArrowArray,
    schema: ArrowSchema,
}

impl ArrowBatch {
    pub fn from_ts_f64_columns(
        root_name: &str,
        ts_ms: Vec<i64>,
        columns: Vec<(&str, Vec<f64>)>,
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

        export_ts_f64_table_to_c(root_name, ts_ms, columns, &mut array, &mut schema)?;
        Ok(Self { array, schema })
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
    cols: Vec<Vec<f64>>,

    // CStrings for schema
    root_format: CString,
    ts_format: CString,
    f64_format: CString,
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

impl PrivateData {
    fn new(root_name: &str, ts: Vec<i64>, columns: Vec<(&str, Vec<f64>)>) -> Result<Self, ArrowExportError> {
        let mut field_names: Vec<CString> = Vec::with_capacity(1 + columns.len());
        field_names.push(CString::new("ts_ms").map_err(|_| ArrowExportError)?);
        for (name, _) in &columns {
            field_names.push(CString::new(*name).map_err(|_| ArrowExportError)?);
        }

        let mut cols: Vec<Vec<f64>> = Vec::with_capacity(columns.len());
        for (_, col) in columns {
            cols.push(col);
        }

        Ok(Self {
            refcnt: AtomicUsize::new(0),
            ts,
            cols,
            root_format: CString::new("+s").map_err(|_| ArrowExportError)?,
            ts_format: CString::new("l").map_err(|_| ArrowExportError)?,
            f64_format: CString::new("g").map_err(|_| ArrowExportError)?,
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
        for c in &self.cols {
            if c.len() != n_rows {
                return Err(ArrowExportError);
            }
        }

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

        // Children 1..: Float64 columns
        for (idx, col) in self.cols.iter().enumerate() {
            let child_idx = 1 + idx;
            self.child_buffers
                .push(vec![ptr::null(), col.as_ptr() as *const c_void]);
            self.child_arrays.push(Box::new(ArrowArray {
                length: n_rows as i64,
                null_count: 0,
                offset: 0,
                n_buffers: 2,
                n_children: 0,
                buffers: self.child_buffers[child_idx].as_mut_ptr(),
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: Some(release_noop_array),
                private_data: ptr::null_mut(),
            }));
            self.child_schemas.push(Box::new(ArrowSchema {
                format: self.f64_format.as_ptr(),
                name: self.field_names[child_idx].as_ptr(),
                metadata: ptr::null(),
                flags: 0,
                n_children: 0,
                children: ptr::null_mut(),
                dictionary: ptr::null_mut(),
                release: Some(release_noop_schema),
                private_data: ptr::null_mut(),
            }));
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
/// - `columns`: `[(name, values)]`, each column is Float64 and must have the same length as `ts_ms`
///
/// On success, fills `out_array/out_schema`. The consumer must call `out_array.release(out_array)`
/// and `out_schema.release(out_schema)` exactly once.
pub fn export_ts_f64_table_to_c(
    root_name: &str,
    ts_ms: Vec<i64>,
    columns: Vec<(&str, Vec<f64>)>,
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
