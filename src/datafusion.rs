// SPDX-License-Identifier: AGPL-3.0-or-later
//
// DataFusion integration (read-only SQL).

use crate::db::{ColumnData, ColumnType, NanoTsDb};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_expr::{create_udf, ColumnarValue, ScalarFunctionImplementation, Volatility};
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::sync::Arc;
use tokio::task;

pub fn query_sql(db: Arc<NanoTsDb>, sql: &str) -> DFResult<Vec<RecordBatch>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| DataFusionError::Execution(format!("tokio runtime build failed: {e}")))?;

    rt.block_on(async move {
        let ctx = SessionContext::new();
        register_time_bucket(&ctx)?;
        register_all_tables(db, &ctx)?;
        let df = ctx.sql(sql).await?;
        df.collect().await
    })
}

fn register_all_tables(db: Arc<NanoTsDb>, ctx: &SessionContext) -> DFResult<()> {
    let tables = db
        .list_tables()
        .map_err(|e| DataFusionError::Execution(format!("list_tables failed: {e}")))?;
    for table in tables {
        let provider = NanoTsTable::try_new(db.clone(), &table)?;
        ctx.register_table(&table, Arc::new(provider))?;
    }
    Ok(())
}

fn register_time_bucket(ctx: &SessionContext) -> DFResult<()> {
    let fun: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "time_bucket expects 2 arguments".to_string(),
            ));
        }
        let arrays = ColumnarValue::values_to_arrays(args)
            .map_err(|e| DataFusionError::Execution(format!("time_bucket args: {e}")))?;
        let interval = arrays[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("time_bucket interval must be Int64".to_string()))?;
        let ts = arrays[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("time_bucket ts must be Int64".to_string()))?;
        let mut out: Vec<Option<i64>> = Vec::with_capacity(ts.len());
        for i in 0..ts.len() {
            if interval.is_null(i) || ts.is_null(i) {
                out.push(None);
                continue;
            }
            let step = interval.value(i);
            if step <= 0 {
                return Err(DataFusionError::Execution(
                    "time_bucket interval must be positive".to_string(),
                ));
            }
            let v = ts.value(i);
            let bucket = v.div_euclid(step).saturating_mul(step);
            out.push(Some(bucket));
        }
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out)) as ArrayRef))
    });

    let udf = create_udf(
        "time_bucket",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        fun,
    );
    ctx.register_udf(udf);
    Ok(())
}

#[derive(Debug)]
struct NanoTsTable {
    db: Arc<NanoTsDb>,
    table: String,
    schema: SchemaRef,
}

impl NanoTsTable {
    fn try_new(db: Arc<NanoTsDb>, table: &str) -> DFResult<Self> {
        let schema = build_table_schema(&db, table)?;
        Ok(Self {
            db,
            table: table.to_string(),
            schema,
        })
    }

    fn build_batches(
        schema: SchemaRef,
        ts: Vec<i64>,
        cols: Vec<ColumnData>,
        limit: Option<usize>,
    ) -> DFResult<Vec<RecordBatch>> {
        let total_rows = ts.len();
        let mut batches = Vec::new();
        let mut offset = 0usize;
        let mut remaining = limit.unwrap_or(total_rows);
        let chunk_size = 65_536usize;

        while offset < total_rows && remaining > 0 {
            let take = chunk_size.min(total_rows - offset).min(remaining);
            let ts_chunk = ts[offset..offset + take].to_vec();
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(2 + cols.len());
            arrays.push(Arc::new(Int64Array::from(ts_chunk.clone())));
            arrays.push(Arc::new(Int64Array::from(ts_chunk)));
            for col in &cols {
                let array = match col {
                    ColumnData::F64(values) => Arc::new(Float64Array::from(
                        values[offset..offset + take].to_vec(),
                    )) as ArrayRef,
                    ColumnData::I64(values) => Arc::new(Int64Array::from(
                        values[offset..offset + take].to_vec(),
                    )) as ArrayRef,
                    ColumnData::Bool(values) => Arc::new(BooleanArray::from(
                        values[offset..offset + take].to_vec(),
                    )) as ArrayRef,
                    ColumnData::Utf8(values) => Arc::new(StringArray::from(
                        values[offset..offset + take].to_vec(),
                    )) as ArrayRef,
                };
                arrays.push(array);
            }
            let batch = RecordBatch::try_new(schema.clone(), arrays)
                .map_err(|e| DataFusionError::Execution(format!("record batch build failed: {e}")))?;
            batches.push(batch);
            offset += take;
            remaining = remaining.saturating_sub(take);
        }

        Ok(batches)
    }
}

#[async_trait]
impl TableProvider for NanoTsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (start, end) = extract_time_range(filters);
        let db = self.db.clone();
        let table = self.table.clone();
        let schema = self.schema.clone();
        let limit_copy = limit;
        let batches = task::spawn_blocking(move || {
            let (ts, cols) = db
                .query_table_range_typed(&table, start, end)
                .map_err(|e| DataFusionError::Execution(format!("query failed: {e}")))?;
            NanoTsTable::build_batches(schema, ts, cols, limit_copy)
        })
        .await
        .map_err(|e| DataFusionError::Execution(format!("blocking task failed: {e}")))?;

        let mut batches = batches?;
        if let Some(proj) = projection {
            let mut projected = Vec::with_capacity(batches.len());
            for batch in batches {
                projected.push(batch.project(proj).map_err(|e| {
                    DataFusionError::Execution(format!("projection failed: {e}"))
                })?);
            }
            batches = projected;
        }

        let schema = batches
            .get(0)
            .map(|b| b.schema())
            .unwrap_or_else(|| self.schema.clone());
        let exec = MemoryExec::try_new(&[batches], schema, None)?;
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let mut out = Vec::with_capacity(filters.len());
        for f in filters {
            if is_time_range_filter(f) {
                out.push(TableProviderFilterPushDown::Exact);
            } else {
                out.push(TableProviderFilterPushDown::Unsupported);
            }
        }
        Ok(out)
    }
}

fn build_table_schema(db: &NanoTsDb, table: &str) -> DFResult<SchemaRef> {
    let schema = db
        .table_schema(table)
        .map_err(|e| DataFusionError::Execution(format!("schema load failed: {e}")))?;
    let mut fields = Vec::with_capacity(2 + schema.columns.len());
    fields.push(Field::new("ts", DataType::Int64, false));
    fields.push(Field::new("ts_ms", DataType::Int64, false));
    for col in schema.columns {
        let dtype = match col.col_type {
            ColumnType::F64 => DataType::Float64,
            ColumnType::I64 => DataType::Int64,
            ColumnType::Bool => DataType::Boolean,
            ColumnType::Utf8 => DataType::Utf8,
        };
        fields.push(Field::new(&col.name, dtype, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn is_time_range_filter(expr: &Expr) -> bool {
    time_range_from_expr(expr).is_some()
}

fn extract_time_range(filters: &[Expr]) -> (i64, i64) {
    let mut start = i64::MIN;
    let mut end = i64::MAX;
    for f in filters {
        if let Some((lo, hi)) = time_range_from_expr(f) {
            if let Some(lo) = lo {
                start = start.max(lo);
            }
            if let Some(hi) = hi {
                end = end.min(hi);
            }
        }
    }
    (start, end)
}

fn time_range_from_expr(expr: &Expr) -> Option<(Option<i64>, Option<i64>)> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = time_range_from_expr(&binary.left)?;
            let right = time_range_from_expr(&binary.right)?;
            Some(merge_ranges(left, right))
        }
        Expr::BinaryExpr(binary) => {
            let op = binary.op;
            let (col, lit) = match (&*binary.left, &*binary.right) {
                (Expr::Column(c), Expr::Literal(v)) => (Some(&c.name), Some(v)),
                (Expr::Literal(v), Expr::Column(c)) => (Some(&c.name), Some(v)),
                _ => (None, None),
            };
            let col = col?;
            if col != "ts" && col != "ts_ms" {
                return None;
            }
            let value = scalar_to_i64(lit?)?;
            match op {
                Operator::Eq => Some((Some(value), Some(value))),
                Operator::Gt => Some((Some(value.saturating_add(1)), None)),
                Operator::GtEq => Some((Some(value), None)),
                Operator::Lt => Some((None, Some(value.saturating_sub(1)))),
                Operator::LtEq => Some((None, Some(value))),
                _ => None,
            }
        }
        _ => None,
    }
}

fn merge_ranges(
    left: (Option<i64>, Option<i64>),
    right: (Option<i64>, Option<i64>),
) -> (Option<i64>, Option<i64>) {
    let lo = match (left.0, right.0) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    let hi = match (left.1, right.1) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    (lo, hi)
}

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v / 1_000),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v / 1_000_000),
        _ => None,
    }
}
