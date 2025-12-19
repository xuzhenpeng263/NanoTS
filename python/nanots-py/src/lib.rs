// SPDX-License-Identifier: AGPL-3.0-or-later

use nanots_core::arrow::{ArrowArray, ArrowSchema};
use nanots_core::db::{ColumnData, ColumnType, Value};
use nanots_core::{NanoTsDb, NanoTsOptions};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyAnyMethods, PyBool, PyDict, PyFloat, PyInt, PyString};
use std::sync::Mutex;
use std::time::Duration;

#[pyclass]
struct Db {
    inner: Mutex<NanoTsDb>,
    path: String,
}

#[pymethods]
impl Db {
    #[new]
    #[pyo3(signature = (path, retention_ms=None))]
    fn new(path: &str, retention_ms: Option<u64>) -> PyResult<Self> {
        let opts = NanoTsOptions {
            retention: retention_ms.map(Duration::from_millis),
            ..Default::default()
        };
        let db = NanoTsDb::open(path, opts).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self {
            inner: Mutex::new(db),
            path: path.to_string(),
        })
    }

    fn create_table(&self, table: &str, columns: Vec<String>) -> PyResult<()> {
        let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.create_table(table, &cols)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn create_table_typed(&self, table: &str, columns: Vec<(String, String)>) -> PyResult<()> {
        let mut cols: Vec<(&str, ColumnType)> = Vec::with_capacity(columns.len());
        for (name, ty) in &columns {
            let col_type = parse_column_type(ty)?;
            cols.push((name.as_str(), col_type));
        }
        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.create_table_typed(table, &cols)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn append(&self, series: &str, ts_ms: i64, value: f64) -> PyResult<u64> {
        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.append(series, ts_ms, value)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn append_row(&self, table: &str, ts_ms: i64, values: Vec<f64>) -> PyResult<u64> {
        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.append_row(table, ts_ms, &values)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn append_row_typed(&self, py: Python<'_>, table: &str, ts_ms: i64, values: Vec<PyObject>) -> PyResult<u64> {
        let mut out: Vec<Value> = Vec::with_capacity(values.len());
        for v in values {
            let value = py_to_value(&v.bind(py))?;
            out.push(value);
        }
        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.append_row_typed(table, ts_ms, &out)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn flush(&self) -> PyResult<()> {
        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.flush().map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn pack_table(&self, table: &str, target_segment_points: usize) -> PyResult<()> {
        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.pack_table(table, target_segment_points)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn query_range(&self, series: &str, start_ms: i64, end_ms: i64) -> PyResult<Vec<(i64, f64)>> {
        let db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        let pts = db
            .query_range(series, start_ms, end_ms)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(pts.into_iter().map(|p| (p.ts_ms, p.value)).collect())
    }

    fn query_table_range_columns(
        &self,
        table: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> PyResult<(Vec<i64>, Vec<Vec<f64>>)> {
        let db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        db.query_table_range_columns(table, start_ms, end_ms)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn query_table_range_typed(
        &self,
        py: Python<'_>,
        table: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> PyResult<(Vec<i64>, Vec<Vec<PyObject>>)> {
        let db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        let (ts, cols) = db
            .query_table_range_typed(table, start_ms, end_ms)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut out_cols: Vec<Vec<PyObject>> = Vec::with_capacity(cols.len());
        for col in cols {
            match col {
                ColumnData::F64(values) => out_cols.push(
                    values
                        .into_iter()
                        .map(|v| v.map(|x| x.to_object(py)).unwrap_or_else(|| py.None()))
                        .collect(),
                ),
                ColumnData::I64(values) => out_cols.push(
                    values
                        .into_iter()
                        .map(|v| v.map(|x| x.to_object(py)).unwrap_or_else(|| py.None()))
                        .collect(),
                ),
                ColumnData::Bool(values) => out_cols.push(
                    values
                        .into_iter()
                        .map(|v| v.map(|x| x.to_object(py)).unwrap_or_else(|| py.None()))
                        .collect(),
                ),
                ColumnData::Utf8(values) => out_cols.push(
                    values
                        .into_iter()
                        .map(|v| v.map(|x| x.to_object(py)).unwrap_or_else(|| py.None()))
                        .collect(),
                ),
            }
        }
        Ok((ts, out_cols))
    }

    /// Returns two PyCapsules: (arrow_array_capsule, arrow_schema_capsule).
    /// Import in Python with `pyarrow.Array._import_from_c(...)`.
    fn query_table_range_arrow_capsules(
        &self,
        py: Python<'_>,
        table: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> PyResult<(PyObject, PyObject)> {
        let db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        let schema = db
            .table_schema(table)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let (ts, cols) = db
            .query_table_range_typed(table, start_ms, end_ms)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let mut columns: Vec<(&str, ColumnData)> = Vec::with_capacity(schema.columns.len());
        for (col_schema, col) in schema.columns.iter().zip(cols) {
            columns.push((col_schema.name.as_str(), col));
        }

        let mut array = Box::new(unsafe { std::mem::zeroed::<ArrowArray>() });
        let mut schema_out = Box::new(unsafe { std::mem::zeroed::<ArrowSchema>() });
        nanots_core::arrow::export_ts_table_to_c(
            table,
            ts,
            columns,
            &mut *array,
            &mut *schema_out,
        )
            .map_err(|_| PyRuntimeError::new_err("arrow export failed"))?;

        unsafe extern "C" fn capsule_destructor_array(capsule: *mut pyo3::ffi::PyObject) {
            let ptr = pyo3::ffi::PyCapsule_GetPointer(capsule, b"arrow_array\0".as_ptr() as *const _);
            if !ptr.is_null() {
                let array = ptr as *mut ArrowArray;
                if let Some(release) = (*array).release {
                    release(array);
                }
                drop(Box::from_raw(array));
            }
        }

        unsafe extern "C" fn capsule_destructor_schema(capsule: *mut pyo3::ffi::PyObject) {
            let ptr = pyo3::ffi::PyCapsule_GetPointer(capsule, b"arrow_schema\0".as_ptr() as *const _);
            if !ptr.is_null() {
                let schema = ptr as *mut ArrowSchema;
                if let Some(release) = (*schema).release {
                    release(schema);
                }
                drop(Box::from_raw(schema));
            }
        }

        let array_ptr = Box::into_raw(array) as *mut std::ffi::c_void;
        let schema_ptr = Box::into_raw(schema_out) as *mut std::ffi::c_void;

        let array_capsule = unsafe {
            PyObject::from_owned_ptr(
                py,
                pyo3::ffi::PyCapsule_New(
                    array_ptr,
                    b"arrow_array\0".as_ptr() as *const _,
                    Some(capsule_destructor_array),
                ),
            )
        };
        let schema_capsule = unsafe {
            PyObject::from_owned_ptr(
                py,
                pyo3::ffi::PyCapsule_New(
                    schema_ptr,
                    b"arrow_schema\0".as_ptr() as *const _,
                    Some(capsule_destructor_schema),
                ),
            )
        };

        // Follow PyArrow's `_import_from_c_capsule(schema_capsule, array_capsule)` convention.
        Ok((schema_capsule, array_capsule))
    }

    /// Returns an Arrow C Stream capsule for SQL query results.
    /// Import in Python with `pyarrow.RecordBatchReader._import_from_c_capsule(capsule)`.
    #[cfg(feature = "datafusion")]
    fn query_sql_arrow_stream_capsule(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        use arrow::ffi_stream::FFI_ArrowArrayStream;
        use datafusion::arrow::record_batch::{RecordBatchIterator, RecordBatchReader};
        use std::sync::Arc;

        let db = Arc::new(
            NanoTsDb::open(&self.path, NanoTsOptions::default())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        );
        let batches = nanots_core::datafusion::query_sql(db, sql)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let schema = batches
            .get(0)
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        let stream = Box::new(FFI_ArrowArrayStream::new(reader));

        unsafe extern "C" fn capsule_destructor_stream(capsule: *mut pyo3::ffi::PyObject) {
            let ptr =
                pyo3::ffi::PyCapsule_GetPointer(capsule, b"arrow_array_stream\0".as_ptr() as *const _);
            if !ptr.is_null() {
                drop(Box::from_raw(ptr as *mut FFI_ArrowArrayStream));
            }
        }

        let stream_ptr = Box::into_raw(stream) as *mut std::ffi::c_void;
        let stream_capsule = unsafe {
            PyObject::from_owned_ptr(
                py,
                pyo3::ffi::PyCapsule_New(
                    stream_ptr,
                    b"arrow_array_stream\0".as_ptr() as *const _,
                    Some(capsule_destructor_stream),
                ),
            )
        };
        Ok(stream_capsule)
    }

fn stats(&self, py: Python<'_>, table: &str) -> PyResult<PyObject> {
        let db = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        let stats = db
            .table_stats(table)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let d = PyDict::new(py);
        d.set_item("rows", stats.rows)?;
        d.set_item("segments", stats.segments)?;
        d.set_item("columns", stats.columns)?;

        d.set_item("raw_ts_bytes", stats.raw_ts_bytes)?;
        d.set_item("raw_value_bytes", stats.raw_value_bytes)?;
        d.set_item("raw_total_bytes", stats.raw_ts_bytes + stats.raw_value_bytes)?;

        d.set_item("stored_ts_bytes", stats.stored_ts_bytes)?;
        d.set_item("stored_value_bytes", stats.stored_value_bytes)?;
        d.set_item(
            "stored_total_bytes",
            stats.stored_ts_bytes + stats.stored_value_bytes,
        )?;

        d.set_item("header_bytes", stats.header_bytes)?;
        d.set_item("ntt_file_bytes", stats.ntt_file_bytes)?;
        d.set_item("idx_file_bytes", stats.idx_file_bytes)?;
        d.set_item("schema_file_bytes", stats.schema_file_bytes)?;

        let on_disk = stats.ntt_file_bytes + stats.idx_file_bytes;
        d.set_item("on_disk_bytes", on_disk)?;
        if on_disk > 0 {
            let raw_total = stats.raw_ts_bytes + stats.raw_value_bytes;
            d.set_item("compression_ratio_total_x", (raw_total as f64) / (on_disk as f64))?;
            // Blob-level ratios (more meaningful than comparing against the whole .ntt file).
            if stats.stored_ts_bytes > 0 {
                d.set_item(
                    "compression_ratio_ts_blob_x",
                    (stats.raw_ts_bytes as f64) / (stats.stored_ts_bytes as f64),
                )?;
            }
            if stats.stored_value_bytes > 0 {
                d.set_item(
                    "compression_ratio_value_blob_x",
                    (stats.raw_value_bytes as f64) / (stats.stored_value_bytes as f64),
                )?;
            }
            // Kept for backward compatibility with earlier scripts; note `.ntt` includes ts+headers.
            if stats.ntt_file_bytes > 0 {
                d.set_item(
                    "compression_ratio_values_x",
                    (stats.raw_value_bytes as f64) / (stats.ntt_file_bytes as f64),
                )?;
            }
        }

        Ok(d.to_object(py))
    }

    fn table_time_range(&self, py: Python<'_>, table: &str) -> PyResult<PyObject> {
        let db = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        let range = db
            .table_time_range(table)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        match range {
            Some((min_ts, max_ts)) => Ok((min_ts, max_ts).to_object(py)),
            None => Ok(py.None()),
        }
    }
}

fn parse_column_type(ty: &str) -> PyResult<ColumnType> {
    let norm = ty.trim().to_ascii_lowercase();
    match norm.as_str() {
        "f64" | "float" | "double" => Ok(ColumnType::F64),
        "i64" | "int" | "integer" => Ok(ColumnType::I64),
        "bool" | "boolean" => Ok(ColumnType::Bool),
        "str" | "string" | "utf8" => Ok(ColumnType::Utf8),
        _ => Err(PyRuntimeError::new_err("unsupported column type")),
    }
}

fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if obj.is_instance_of::<PyBool>() {
        return Ok(Value::Bool(obj.extract::<bool>()?));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(Value::I64(obj.extract::<i64>()?));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Value::F64(obj.extract::<f64>()?));
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(Value::Utf8(obj.extract::<String>()?));
    }
    Err(PyRuntimeError::new_err(format!(
        "unsupported value type: {}",
        obj.get_type().name()?
    )))
}

#[pymodule]
fn nanots(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Db>()?;
    Ok(())
}
