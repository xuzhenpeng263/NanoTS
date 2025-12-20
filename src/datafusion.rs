// SPDX-License-Identifier: AGPL-3.0-or-later
//
// DataFusion integration (read-only SQL).

use crate::db::{ColumnData, ColumnPredicate, ColumnPredicateOp, ColumnPredicateValue, ColumnType, NanoTsDb};
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
use std::sync::{Arc, OnceLock};
use tokio::task;

pub fn query_sql(db: Arc<NanoTsDb>, sql: &str) -> DFResult<Vec<RecordBatch>> {
    let rt = runtime()?;
    rt.block_on(async move {
        let ctx = SessionContext::new();
        register_udfs(&ctx)?;
        register_all_tables(db, &ctx)?;
        let df = ctx.sql(sql).await?;
        df.collect().await
    })
}

fn runtime() -> DFResult<&'static tokio::runtime::Runtime> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    match RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tokio runtime build failed: {e}"))
    }) {
        Ok(rt) => Ok(rt),
        Err(msg) => Err(DataFusionError::Execution(msg.clone())),
    }
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

fn register_udfs(ctx: &SessionContext) -> DFResult<()> {
    register_time_bucket(ctx)?;
    register_delta(ctx)?;
    register_rate(ctx)?;
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

fn register_delta(ctx: &SessionContext) -> DFResult<()> {
    let fun: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "delta expects 2 arguments".to_string(),
            ));
        }
        let arrays = ColumnarValue::values_to_arrays(args)
            .map_err(|e| DataFusionError::Execution(format!("delta args: {e}")))?;
        if let (Some(a), Some(b)) = (
            arrays[0].as_any().downcast_ref::<Float64Array>(),
            arrays[1].as_any().downcast_ref::<Float64Array>(),
        ) {
            let mut out = Vec::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_null(i) || b.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(a.value(i) - b.value(i)));
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(Float64Array::from(out)) as ArrayRef));
        }
        if let (Some(a), Some(b)) = (
            arrays[0].as_any().downcast_ref::<Int64Array>(),
            arrays[1].as_any().downcast_ref::<Int64Array>(),
        ) {
            let mut out = Vec::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_null(i) || b.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(a.value(i) - b.value(i)));
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out)) as ArrayRef));
        }
        Err(DataFusionError::Execution(
            "delta expects (Float64, Float64) or (Int64, Int64)".to_string(),
        ))
    });

    let udf = create_udf(
        "delta",
        vec![DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        fun.clone(),
    );
    ctx.register_udf(udf);

    let udf_i64 = create_udf(
        "delta_i64",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        fun,
    );
    ctx.register_udf(udf_i64);
    Ok(())
}

fn register_rate(ctx: &SessionContext) -> DFResult<()> {
    let fun: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
        if args.len() != 3 {
            return Err(DataFusionError::Execution(
                "rate expects 3 arguments".to_string(),
            ));
        }
        let arrays = ColumnarValue::values_to_arrays(args)
            .map_err(|e| DataFusionError::Execution(format!("rate args: {e}")))?;
        let a = arrays[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("rate expects Float64 values".to_string()))?;
        let b = arrays[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("rate expects Float64 values".to_string()))?;
        let dt = arrays[2]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("rate expects Int64 dt".to_string()))?;
        let mut out = Vec::with_capacity(a.len());
        for i in 0..a.len() {
            if a.is_null(i) || b.is_null(i) || dt.is_null(i) {
                out.push(None);
                continue;
            }
            let dt_ms = dt.value(i);
            if dt_ms == 0 {
                out.push(None);
                continue;
            }
            out.push(Some((a.value(i) - b.value(i)) / (dt_ms as f64)));
        }
        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(out)) as ArrayRef))
    });

    let udf = create_udf(
        "rate",
        vec![DataType::Float64, DataType::Float64, DataType::Int64],
        DataType::Float64,
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
        let predicates = extract_column_predicates(filters);
        let db = self.db.clone();
        let table = self.table.clone();
        let schema = self.schema.clone();
        let limit_copy = limit;
        let batches = task::spawn_blocking(move || {
            let (ts, cols) = db
                .query_table_range_typed_snapshot_with_predicates(&table, start, end, &predicates)
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
            if has_value_predicate(f) {
                out.push(TableProviderFilterPushDown::Inexact);
            } else if is_time_range_filter(f) {
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

fn extract_column_predicates(filters: &[Expr]) -> Vec<ColumnPredicate> {
    let mut out = Vec::new();
    for f in filters {
        collect_predicates(f, &mut out);
    }
    out
}

fn is_time_range_filter(expr: &Expr) -> bool {
    time_range_from_expr(expr).is_some()
}

fn has_value_predicate(expr: &Expr) -> bool {
    let mut out = Vec::new();
    collect_predicates(expr, &mut out);
    !out.is_empty()
}

fn time_range_from_expr(expr: &Expr) -> Option<(Option<i64>, Option<i64>)> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = time_range_from_expr(&binary.left);
            let right = time_range_from_expr(&binary.right);
            match (left, right) {
                (None, None) => None,
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (Some(l), Some(r)) => Some(merge_ranges(l, r)),
            }
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

fn collect_predicates(expr: &Expr, out: &mut Vec<ColumnPredicate>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_predicates(&binary.left, out);
            collect_predicates(&binary.right, out);
        }
        Expr::BinaryExpr(binary) => {
            let op = match binary.op {
                Operator::Eq => ColumnPredicateOp::Eq,
                Operator::Gt => ColumnPredicateOp::Gt,
                Operator::GtEq => ColumnPredicateOp::GtEq,
                Operator::Lt => ColumnPredicateOp::Lt,
                Operator::LtEq => ColumnPredicateOp::LtEq,
                _ => return,
            };
            let (col, lit) = match (&*binary.left, &*binary.right) {
                (Expr::Column(c), Expr::Literal(v)) => (Some(&c.name), Some(v)),
                (Expr::Literal(v), Expr::Column(c)) => (Some(&c.name), Some(v)),
                _ => (None, None),
            };
            let col = match col {
                Some(col) if col != "ts" && col != "ts_ms" => col,
                _ => return,
            };
            let value = match scalar_to_predicate_value(lit.unwrap()) {
                Some(v) => v,
                None => return,
            };
            out.push(ColumnPredicate {
                column: col.to_string(),
                op,
                value,
            });
        }
        _ => {}
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

fn scalar_to_predicate_value(value: &ScalarValue) -> Option<ColumnPredicateValue> {
    match value {
        ScalarValue::Float64(Some(v)) => Some(ColumnPredicateValue::F64(*v)),
        ScalarValue::Float32(Some(v)) => Some(ColumnPredicateValue::F64(*v as f64)),
        ScalarValue::Int64(Some(v)) => Some(ColumnPredicateValue::I64(*v)),
        ScalarValue::Int32(Some(v)) => Some(ColumnPredicateValue::I64(*v as i64)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v)
            .ok()
            .map(ColumnPredicateValue::I64),
        ScalarValue::UInt32(Some(v)) => Some(ColumnPredicateValue::I64(*v as i64)),
        ScalarValue::Boolean(Some(v)) => Some(ColumnPredicateValue::Bool(*v)),
        ScalarValue::Utf8(Some(v)) => Some(ColumnPredicateValue::Utf8(v.clone())),
        _ => None,
    }
}
