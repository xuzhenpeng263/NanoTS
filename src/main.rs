// SPDX-License-Identifier: AGPL-3.0-or-later

pub use nanots::compressor::TimeSeriesCompressor;
use nanots::{ColumnType, NanoTsDb, NanoTsOptions};

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "pack" {
        if args.len() < 4 {
            eprintln!("用法: cargo run --release -- pack <db_path> <table> [--target <N>]");
            std::process::exit(2);
        }
        let db_path = &args[2];
        let table = &args[3];
        let mut target = 8192usize;
        if args.len() >= 6 && args[4] == "--target" {
            target = args[5].parse::<usize>().unwrap_or_else(|_| {
                eprintln!("--target 参数无法解析为整数: {}", args[5]);
                std::process::exit(2);
            });
        }
        let before_size = std::fs::metadata(db_path)
            .map(|m| m.len())
            .unwrap_or(0);
        let mut db = NanoTsDb::open(db_path, NanoTsOptions::default()).unwrap_or_else(|e| {
            eprintln!("打开 DB 失败: {}", e);
            std::process::exit(1);
        });
        db.pack_table(table, target).unwrap_or_else(|e| {
            eprintln!("pack 失败: {}", e);
            std::process::exit(1);
        });
        let after_size = std::fs::metadata(db_path)
            .map(|m| m.len())
            .unwrap_or(0);
        println!(
            "pack 完成: table={} target={} size={} -> {} bytes",
            table, target, before_size, after_size
        );
    } else if args.len() > 1 && args[1] == "info" {
        if args.len() < 3 {
            eprintln!("用法: cargo run --release -- info <db_path>");
            std::process::exit(2);
        }
        let db_path = &args[2];
        let db = NanoTsDb::open(db_path, NanoTsOptions::default()).unwrap_or_else(|e| {
            eprintln!("打开 DB 失败: {}", e);
            std::process::exit(1);
        });
        let tables = db.list_tables().unwrap_or_default();
        println!("path: {}", db_path);
        println!("last_seq: {}", db.last_seq());
        match db.retention() {
            Some(d) => println!("retention_ms: {}", d.as_millis()),
            None => println!("retention_ms: none"),
        }
        match db.get_diagnostics() {
            Ok(diag) => {
                println!("wal_bytes: {}", diag.wal_bytes);
                println!("segment_bytes: {}", diag.total_table_segment_bytes);
                match diag.wal_ratio {
                    Some(r) => println!("wal_ratio: {:.3}", r),
                    None => println!("wal_ratio: none"),
                }
                println!("pending_rows: {}", diag.pending_rows);
                println!("pending_tables: {}", diag.pending_tables);
                println!("writes_per_sec: {}", diag.writes_per_sec);
                println!("last_write_age_ms: {}", diag.last_write_age_ms);
                println!(
                    "maintenance: pack ok={} fail={} retention ok={} fail={}",
                    diag.maintenance.pack_success,
                    diag.maintenance.pack_fail,
                    diag.maintenance.retention_success,
                    diag.maintenance.retention_fail
                );
                if let Some(err) = diag.maintenance.last_pack_error.as_ref() {
                    println!("maintenance_last_pack_error: {}", err);
                }
                if let Some(err) = diag.maintenance.last_retention_error.as_ref() {
                    println!("maintenance_last_retention_error: {}", err);
                }

                if diag.tables.is_empty() {
                    println!("tables: (none)");
                } else {
                    println!("tables:");
                    for t in diag.tables {
                        println!("  - {} (rows={}, segments={})", t.table, t.rows, t.segments);
                        match t.ts_compression_ratio {
                            Some(r) => println!("    ts_compression: {:.3}", r),
                            None => println!("    ts_compression: none"),
                        }
                        match t.value_compression_ratio {
                            Some(r) => println!("    value_compression: {:.3}", r),
                            None => println!("    value_compression: none"),
                        }
                        match t.total_compression_ratio {
                            Some(r) => println!("    total_compression: {:.3}", r),
                            None => println!("    total_compression: none"),
                        }
                        if t.columns.is_empty() {
                            println!("    columns: (none)");
                        } else {
                            println!("    columns:");
                            for c in t.columns {
                                let col_type = format_column_type(c.col_type);
                                let ratio = c
                                    .compression_ratio
                                    .map(|v| format!("{:.3}", v))
                                    .unwrap_or_else(|| "none".to_string());
                                println!(
                                    "      - {} ({}) logical={} stored={} ratio={} nulls={}",
                                    c.column, col_type, c.logical_bytes, c.stored_bytes, ratio, c.nulls
                                );
                            }
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("诊断失败: {}", err);
                if tables.is_empty() {
                    println!("tables: (none)");
                } else {
                    println!("tables:");
                    for t in tables {
                        println!("  - {}", t);
                    }
                }
            }
        }
    } else {
        if args.len() > 1 {
            eprintln!("未知命令: {}", args[1]);
            print_usage_and_exit(2);
        }

        // 简单的演示（无参数时）
        let mut data = Vec::new();
        let start = 1704067200000i64;
        for i in 0..10 {
            data.push(start + (i * 1000) as i64);
        }

        println!("原始数据: {:?}", data);

        let compressed = TimeSeriesCompressor::compress(&data);
        println!("压缩后大小: {} 字节", compressed.len());
        println!(
            "压缩比: {:.2}x",
            (data.len() * 8) as f64 / compressed.len() as f64
        );

        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        println!("解压后: {:?}", decompressed);
        println!("数据一致性: {}", data == decompressed);

        println!("  cargo run --release -- pack <db_path> <table> [--target N]  # 手动 Compaction");
        println!("  cargo run --release -- info <db_path>                       # 查看 DB 信息");
    }
}

fn format_column_type(col_type: ColumnType) -> &'static str {
    match col_type {
        ColumnType::F64 => "F64",
        ColumnType::I64 => "I64",
        ColumnType::Bool => "Bool",
        ColumnType::Utf8 => "Utf8",
    }
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!("用法:");
    eprintln!("  cargo run --release -- pack <db_path> <table> [--target N]");
    eprintln!("  cargo run --release -- info <db_path>");
    std::process::exit(code)
}
