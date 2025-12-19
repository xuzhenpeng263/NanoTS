// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NanotsHandle NanotsHandle;

typedef struct NanotsPoint {
  int64_t ts_ms;
  double value;
} NanotsPoint;

// Apache Arrow C Data Interface (https://arrow.apache.org/docs/format/CDataInterface.html)
typedef struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
} ArrowArray;

typedef struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
} ArrowSchema;

// Opens/creates a NanoTS DB at `path`.
// `retention_ms` < 0 disables retention.
NanotsHandle* nanots_open(const char* path, int64_t retention_ms);

void nanots_close(NanotsHandle* handle);

// Appends a single point. Returns the WAL sequence number on success (>= 1), or a negative error code.
int64_t nanots_append(NanotsHandle* handle, const char* series, int64_t ts_ms, double value);

// Flushes all buffered points to disk and checkpoints/truncates WAL. Returns 0 on success.
int nanots_flush(NanotsHandle* handle);

// Creates a Float64-only table schema (columns are value fields; `ts_ms` is implicit).
// `columns` is an array of `ncols` UTF-8 C strings.
int nanots_create_table(NanotsHandle* handle, const char* table, const char** columns, size_t ncols);

// Appends a full row for a multi-column table. Returns seq (>=1) on success.
int64_t nanots_append_row(
    NanotsHandle* handle,
    const char* table,
    int64_t ts_ms,
    const double* values,
    size_t nvals);

// Queries points in [start_ms, end_ms]. Returns a heap-allocated array and writes its length to `out_len`.
// Free the returned pointer with `nanots_query_free`.
NanotsPoint* nanots_query_range(
    NanotsHandle* handle,
    const char* series,
    int64_t start_ms,
    int64_t end_ms,
    size_t* out_len);

void nanots_query_free(NanotsPoint* ptr, size_t len);

size_t nanots_point_size(void);

// Exports a table query result as Arrow C Data Interface:
// root struct = { ts_ms: int64, <schema columns...>: float64 }
// Returns 0 on success. On success, caller must call:
//   out_array->release(out_array); out_schema->release(out_schema);
int nanots_query_table_range_arrow(
    NanotsHandle* handle,
    const char* table,
    int64_t start_ms,
    int64_t end_ms,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema);

#ifdef __cplusplus
} // extern "C"
#endif
