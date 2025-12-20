// SPDX-License-Identifier: AGPL-3.0-or-later
//
// NanoTS (Community Edition)
// An embeddable, append-only time-series storage kernel.

pub mod compressor;
pub mod dbfile;
pub mod db;
pub mod storage;
pub mod wal;

#[cfg(feature = "arrow")]
pub mod arrow;

#[cfg(feature = "datafusion")]
pub mod datafusion;

mod c_api;

pub use crate::db::{AutoMaintenanceOptions, NanoTsDb, NanoTsDbShared, NanoTsOptions, Point};
pub use crate::storage::TableStats;
