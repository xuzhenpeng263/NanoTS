// SPDX-License-Identifier: AGPL-3.0-or-later

use nanots_core::arrow::{ArrowArray, ArrowSchema};
use nanots_core::db::{AutoMaintenanceOptions, ColumnData, ColumnType, Value};
use nanots_core::{NanoTsDb, NanoTsOptions};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::BoundObject;
use pyo3::types::{PyAny, PyAnyMethods, PyBool, PyDict, PyFloat, PyInt, PyString};
#[cfg(feature = "arrow")]
use pyo3::types::PyCapsule;
use std::sync::Mutex;
use std::time::Duration;
#[cfg(feature = "arrow")]
use std::ffi::c_void;
#[cfg(feature = "datafusion")]
use std::thread;

#[cfg(feature = "arrow")]
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, StructArray};
#[cfg(feature = "arrow")]
use arrow::ffi::{self, FFI_ArrowArray, FFI_ArrowSchema};

#[pyclass]
struct Db {
    inner: Mutex<NanoTsDb>,
    path: String,
}

#[pymethods]
impl Db {
    #[new]
    #[pyo3(signature = (path, retention_ms=None, auto_maintenance=None))]
    fn new(py: Python<'_>, path: &str, retention_ms: Option<u64>, auto_maintenance: Option<PyObject>) -> PyResult<Self> {
        let opts = NanoTsOptions {
            retention: retention_ms.map(Duration::from_millis),
            auto_maintenance: parse_auto_maintenance(py, auto_maintenance)?,
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
                        .map(|v| option_to_pyobject(py, v))
                        .collect::<PyResult<Vec<_>>>()?,
                ),
                ColumnData::I64(values) => out_cols.push(
                    values
                        .into_iter()
                        .map(|v| option_to_pyobject(py, v))
                        .collect::<PyResult<Vec<_>>>()?,
                ),
                ColumnData::Bool(values) => out_cols.push(
                    values
                        .into_iter()
                        .map(|v| option_to_pyobject(py, v))
                        .collect::<PyResult<Vec<_>>>()?,
                ),
                ColumnData::Utf8(values) => out_cols.push(
                    values
                        .into_iter()
                        .map(|v| option_to_pyobject(py, v))
                        .collect::<PyResult<Vec<_>>>()?,
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

    /// Append a batch from Arrow C Data Interface capsules.
    /// Accepts (schema_capsule, array_capsule) like PyArrow's `__arrow_c_array__`.
    #[cfg(feature = "arrow")]
    fn append_rows_arrow_capsules(
        &self,
        py: Python<'_>,
        table: &str,
        schema_capsule: PyObject,
        array_capsule: PyObject,
    ) -> PyResult<u64> {
        let schema_capsule = schema_capsule.bind(py).downcast::<PyCapsule>()?;
        let array_capsule = array_capsule.bind(py).downcast::<PyCapsule>()?;

        let schema_ptr = capsule_pointer(schema_capsule, "arrow_schema")?;
        let array_ptr = capsule_pointer(array_capsule, "arrow_array")?;
        if schema_ptr.is_null() || array_ptr.is_null() {
            return Err(PyRuntimeError::new_err("null Arrow capsule pointer"));
        }

        let schema_ptr = schema_ptr.cast::<FFI_ArrowSchema>();
        let ffi_array = unsafe { FFI_ArrowArray::from_raw(array_ptr.cast()) };
        let mut array_data = unsafe { ffi::from_ffi(ffi_array, &*schema_ptr) }
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        array_data.align_buffers();
        let struct_array = StructArray::from(array_data);

        let ts_array = struct_array
            .column_by_name("ts_ms")
            .or_else(|| struct_array.column_by_name("ts"))
            .ok_or_else(|| PyRuntimeError::new_err("missing ts_ms column"))?;
        let ts_array = ts_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| PyRuntimeError::new_err("ts_ms must be int64"))?;
        let mut ts_ms = Vec::with_capacity(ts_array.len());
        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                return Err(PyRuntimeError::new_err("ts_ms contains null"));
            }
            ts_ms.push(ts_array.value(i));
        }

        let mut db = self.inner.lock().map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        let schema = db
            .table_schema(table)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut columns: Vec<ColumnData> = Vec::with_capacity(schema.columns.len());

        for col in &schema.columns {
            let array = struct_array
                .column_by_name(&col.name)
                .ok_or_else(|| PyRuntimeError::new_err(format!("missing column {}", col.name)))?;
            let data = match col.col_type {
                ColumnType::F64 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| PyRuntimeError::new_err("f64 column type mismatch"))?;
                    let mut out = Vec::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        out.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                    }
                    ColumnData::F64(out)
                }
                ColumnType::I64 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| PyRuntimeError::new_err("i64 column type mismatch"))?;
                    let mut out = Vec::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        out.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                    }
                    ColumnData::I64(out)
                }
                ColumnType::Bool => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| PyRuntimeError::new_err("bool column type mismatch"))?;
                    let mut out = Vec::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        out.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                    }
                    ColumnData::Bool(out)
                }
                ColumnType::Utf8 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| PyRuntimeError::new_err("utf8 column type mismatch"))?;
                    let mut out = Vec::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        out.push(if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i).to_string())
                        });
                    }
                    ColumnData::Utf8(out)
                }
            };
            columns.push(data);
        }

        if columns.iter().any(|col| match col {
            ColumnData::F64(v) => v.len() != ts_ms.len(),
            ColumnData::I64(v) => v.len() != ts_ms.len(),
            ColumnData::Bool(v) => v.len() != ts_ms.len(),
            ColumnData::Utf8(v) => v.len() != ts_ms.len(),
        }) {
            return Err(PyRuntimeError::new_err("column length mismatch"));
        }

        db.append_table_batch(table, &ts_ms, &columns)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Returns an Arrow C Stream capsule for SQL query results.
    /// Import in Python with `pyarrow.RecordBatchReader._import_from_c_capsule(capsule)`.
    #[cfg(feature = "datafusion")]
    fn query_sql_arrow_stream_capsule(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let batches = self.query_sql_batches(sql)?;
        build_arrow_stream_capsule(py, batches)
    }

    #[cfg(feature = "datafusion")]
    fn query_sql_async(&self, py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
        let path = self.path.clone();
        let sql = sql.to_string();
        let loop_obj: Py<PyAny> = py
            .import("asyncio")?
            .call_method0("get_running_loop")?
            .unbind();
        let future: Py<PyAny> = loop_obj.bind(py).call_method0("create_future")?.unbind();

        {
            let mut db = self
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
            db.flush()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }

        let loop_handle = loop_obj.clone_ref(py);
        let future_handle = future.clone_ref(py);
        thread::spawn(move || {
            let result = (|| -> PyResult<PyObject> {
                let db = std::sync::Arc::new(
                    NanoTsDb::open(&path, NanoTsOptions::default())
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
                );
                let batches = nanots_core::datafusion::query_sql(db, &sql)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Python::with_gil(|py| build_arrow_stream_capsule(py, batches))
            })();
            Python::with_gil(|py| {
                let loop_obj = loop_handle.bind(py);
                let future_obj = future_handle.bind(py);
                match result {
                    Ok(val) => {
                        let _ = loop_obj.call_method1(
                            "call_soon_threadsafe",
                            (future_obj.getattr("set_result")?, val),
                        );
                    }
                    Err(err) => {
                        let err_obj = err.value(py).clone().into_any().unbind();
                        let _ = loop_obj.call_method1(
                            "call_soon_threadsafe",
                            (future_obj.getattr("set_exception")?, err_obj),
                        );
                    }
                }
                Ok::<(), PyErr>(())
            })
            .ok();
        });
        Ok(future)
    }

    #[cfg(feature = "datafusion")]
    fn query_sql_to_pandas(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let capsule = self.query_sql_arrow_stream_capsule(py, sql)?;
        let pyarrow = py.import("pyarrow")?;
        let reader = pyarrow
            .getattr("RecordBatchReader")?
            .call_method1("_import_from_c_capsule", (capsule,))?;
        let table = reader.call_method0("read_all")?;
        let df = table.call_method0("to_pandas")?;
        Ok(df.into())
    }

    #[cfg(feature = "datafusion")]
    fn query_sql_to_polars(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let capsule = self.query_sql_arrow_stream_capsule(py, sql)?;
        let pyarrow = py.import("pyarrow")?;
        let reader = pyarrow
            .getattr("RecordBatchReader")?
            .call_method1("_import_from_c_capsule", (capsule,))?;
        let table = reader.call_method0("read_all")?;
        let polars = py.import("polars")?;
        let df = polars.call_method1("from_arrow", (table,))?;
        Ok(df.into())
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

        Ok(d.into_any().unbind())
    }

    fn diagnose(&self, py: Python<'_>) -> PyResult<PyObject> {
        let db = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
        let diag = db
            .diagnose()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let d = PyDict::new(py);
        d.set_item("wal_bytes", diag.wal_bytes)?;
        d.set_item("total_table_segment_bytes", diag.total_table_segment_bytes)?;
        if let Some(ratio) = diag.wal_ratio {
            d.set_item("wal_ratio", ratio)?;
        } else {
            d.set_item("wal_ratio", py.None())?;
        }
        d.set_item("pending_rows", diag.pending_rows)?;
        d.set_item("pending_tables", diag.pending_tables)?;
        d.set_item("writes_per_sec", diag.writes_per_sec)?;
        d.set_item("last_write_age_ms", diag.last_write_age_ms)?;
        Ok(d.into_any().unbind())
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
            Some((min_ts, max_ts)) => Ok((min_ts, max_ts)
                .into_pyobject(py)?
                .into_any()
                .unbind()),
            None => Ok(py.None()),
        }
    }
}

#[cfg(feature = "datafusion")]
impl Db {
    fn query_sql_batches(
        &self,
        sql: &str,
    ) -> PyResult<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        {
            let mut db = self
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("db lock poisoned"))?;
            db.flush()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }
        let db = std::sync::Arc::new(
            NanoTsDb::open(&self.path, NanoTsOptions::default())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        );
        nanots_core::datafusion::query_sql(db, sql)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

fn option_to_pyobject<'py, T>(py: Python<'py>, value: Option<T>) -> PyResult<PyObject>
where
    T: IntoPyObject<'py>,
    <T as IntoPyObject<'py>>::Error: Into<PyErr>,
{
    match value {
        Some(v) => Ok(v
            .into_pyobject(py)
            .map_err(Into::into)?
            .into_any()
            .unbind()),
        None => Ok(py.None()),
    }
}

#[cfg(feature = "datafusion")]
fn build_arrow_stream_capsule(
    py: Python<'_>,
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
) -> PyResult<PyObject> {
    use arrow::ffi_stream::FFI_ArrowArrayStream;
    use datafusion::arrow::record_batch::{RecordBatchIterator, RecordBatchReader};
    use std::sync::Arc;

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

fn parse_auto_maintenance(
    py: Python<'_>,
    auto_maintenance: Option<PyObject>,
) -> PyResult<Option<AutoMaintenanceOptions>> {
    let Some(obj) = auto_maintenance else {
        return Ok(Some(AutoMaintenanceOptions::default()));
    };
    let bound = obj.bind(py);
    if bound.is_none() {
        return Ok(Some(AutoMaintenanceOptions::default()));
    }
    if let Ok(flag) = bound.extract::<bool>() {
        return if flag {
            Ok(Some(AutoMaintenanceOptions::default()))
        } else {
            Ok(None)
        };
    }
    if let Ok(dict) = bound.downcast::<PyDict>() {
        let mut opts = AutoMaintenanceOptions::default();
        if let Ok(Some(v)) = dict.get_item("check_interval_ms") {
            opts.check_interval = Duration::from_millis(v.extract::<u64>()?);
        }
        if let Ok(Some(v)) = dict.get_item("retention_check_interval_ms") {
            opts.retention_check_interval = Duration::from_millis(v.extract::<u64>()?);
        }
        if let Ok(Some(v)) = dict.get_item("wal_target_ratio") {
            opts.wal_target_ratio = v.extract::<u64>()?;
        }
        if let Ok(Some(v)) = dict.get_item("wal_min_bytes") {
            opts.wal_min_bytes = v.extract::<u64>()?;
        }
        if let Ok(Some(v)) = dict.get_item("target_segment_points") {
            opts.target_segment_points = v.extract::<usize>()?;
        }
        if let Ok(Some(v)) = dict.get_item("max_writes_per_sec") {
            opts.max_writes_per_sec = v.extract::<u64>()?;
        }
        if let Ok(Some(v)) = dict.get_item("write_load_window_ms") {
            opts.write_load_window = Duration::from_millis(v.extract::<u64>()?);
        }
        if let Ok(Some(v)) = dict.get_item("min_idle_ms") {
            opts.min_idle_time = Duration::from_millis(v.extract::<u64>()?);
        }
        if let Ok(Some(v)) = dict.get_item("max_segments_per_pack") {
            opts.max_segments_per_pack = v.extract::<usize>()?;
        }
        return Ok(Some(opts));
    }
    Err(PyRuntimeError::new_err(
        "auto_maintenance must be a dict, bool, or None",
    ))
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

#[cfg(feature = "arrow")]
fn capsule_pointer(capsule: &Bound<'_, PyCapsule>, name: &str) -> PyResult<*mut c_void> {
    let mut cname = name.as_bytes().to_vec();
    cname.push(0);
    let ptr = unsafe { pyo3::ffi::PyCapsule_GetPointer(capsule.as_ptr(), cname.as_ptr() as *const _) };
    if ptr.is_null() {
        return Err(PyRuntimeError::new_err(format!(
            "invalid capsule name: {}",
            name
        )));
    }
    Ok(ptr)
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
fn nanots_db(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Db>()?;
    Ok(())
}
