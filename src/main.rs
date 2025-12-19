// SPDX-License-Identifier: AGPL-3.0-or-later

pub use nanots::compressor::TimeSeriesCompressor;
use nanots::{NanoTsDb, NanoTsOptions};

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
        if tables.is_empty() {
            println!("tables: (none)");
        } else {
            println!("tables:");
            for t in tables {
                println!("  - {}", t);
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

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!("用法:");
    eprintln!("  cargo run --release -- pack <db_path> <table> [--target N]");
    eprintln!("  cargo run --release -- info <db_path>");
    std::process::exit(code)
}
