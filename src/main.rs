// SPDX-License-Identifier: AGPL-3.0-or-later

pub use nanots::compressor::TimeSeriesCompressor;
use nanots::{NanoTsDb, NanoTsOptions};

mod benchmark;
mod ts64bin;

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "benchmark" {
        benchmark::main();
    } else if args.len() > 1 && args[1] == "benchmark-single" {
        benchmark::main_single_thread();
    } else if args.len() > 1 && args[1] == "benchmark-disk" {
        benchmark::main_disk();
    } else if args.len() > 1 && args[1] == "benchmark-disk-single" {
        benchmark::main_disk_single_thread();
    } else if args.len() > 1 && args[1] == "benchmark-real" {
        let (path, opts) = parse_real_benchmark_args(&args);
        benchmark::main_real(
            &path,
            false,
            opts.limit,
            opts.stride,
            opts.print_stats,
            opts.sort,
            opts.dedup,
        );
    } else if args.len() > 1 && args[1] == "benchmark-real-single" {
        let (path, opts) = parse_real_benchmark_args(&args);
        benchmark::main_real(
            &path,
            true,
            opts.limit,
            opts.stride,
            opts.print_stats,
            opts.sort,
            opts.dedup,
        );
    } else if args.len() > 1 && args[1] == "benchmark-real-disk" {
        let (path, opts) = parse_real_benchmark_args(&args);
        benchmark::main_real_disk(
            &path,
            false,
            opts.limit,
            opts.stride,
            opts.print_stats,
            opts.sort,
            opts.dedup,
        );
    } else if args.len() > 1 && args[1] == "benchmark-real-disk-single" {
        let (path, opts) = parse_real_benchmark_args(&args);
        benchmark::main_real_disk(
            &path,
            true,
            opts.limit,
            opts.stride,
            opts.print_stats,
            opts.sort,
            opts.dedup,
        );
    } else if args.len() > 1 && args[1] == "pack" {
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
        let mut db = NanoTsDb::open(db_path, NanoTsOptions::default()).unwrap_or_else(|e| {
            eprintln!("打开 DB 失败: {}", e);
            std::process::exit(1);
        });
        db.pack_table(table, target).unwrap_or_else(|e| {
            eprintln!("pack 失败: {}", e);
            std::process::exit(1);
        });
        println!("pack 完成: table={} target={}", table, target);
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

        println!("\n要运行基准测试，请使用:");
        println!("  cargo run --release -- benchmark        # 多线程版本");
        println!("  cargo run --release -- benchmark-single # 单线程版本");
        println!("  cargo run --release -- benchmark-disk        # 多线程 + 落盘写入/读取");
        println!("  cargo run --release -- benchmark-disk-single # 单线程 + 落盘写入/读取");
        println!("  cargo run --release -- pack <db_path> <table> [--target N]  # 手动 Compaction");
    }
}

#[derive(Debug, Clone, Copy)]
struct RealBenchOpts {
    limit: Option<usize>,
    stride: usize,
    print_stats: bool,
    sort: bool,
    dedup: bool,
}

fn parse_real_benchmark_args(args: &[String]) -> (std::path::PathBuf, RealBenchOpts) {
    if args.len() < 3 {
        print_usage_and_exit(2);
    }

    let path = std::path::PathBuf::from(&args[2]);
    let mut opts = RealBenchOpts {
        limit: None,
        stride: 1,
        print_stats: true,
        sort: false,
        dedup: false,
    };

    let mut i = 3usize;
    while i < args.len() {
        match args[i].as_str() {
            "--help" | "-h" => print_usage_and_exit(0),
            "--limit" => {
                let v = args.get(i + 1).unwrap_or_else(|| {
                    eprintln!("--limit 需要一个数值参数");
                    print_usage_and_exit(2);
                });
                opts.limit = Some(v.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("--limit 参数无法解析为整数: {}", v);
                    print_usage_and_exit(2);
                }));
                i += 2;
            }
            "--stride" => {
                let v = args.get(i + 1).unwrap_or_else(|| {
                    eprintln!("--stride 需要一个数值参数");
                    print_usage_and_exit(2);
                });
                opts.stride = v.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("--stride 参数无法解析为整数: {}", v);
                    print_usage_and_exit(2);
                });
                if opts.stride == 0 {
                    eprintln!("--stride 必须 >= 1");
                    print_usage_and_exit(2);
                }
                i += 2;
            }
            "--no-stats" => {
                opts.print_stats = false;
                i += 1;
            }
            "--sort" => {
                opts.sort = true;
                i += 1;
            }
            "--dedup" => {
                opts.dedup = true;
                i += 1;
            }
            other => {
                eprintln!("未知参数: {}", other);
                print_usage_and_exit(2);
            }
        }
    }

    if !path.is_file() {
        eprintln!("文件不存在: {}", path.display());
        print_usage_and_exit(2);
    }

    (path, opts)
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!("用法:");
    eprintln!("  cargo run --release -- benchmark");
    eprintln!("  cargo run --release -- benchmark-single");
    eprintln!("  cargo run --release -- benchmark-disk");
    eprintln!("  cargo run --release -- benchmark-disk-single");
    eprintln!();
    eprintln!("  cargo run --release -- benchmark-real <file.ts64bin> [options]");
    eprintln!("  cargo run --release -- benchmark-real-single <file.ts64bin> [options]");
    eprintln!("  cargo run --release -- benchmark-real-disk <file.ts64bin> [options]");
    eprintln!("  cargo run --release -- benchmark-real-disk-single <file.ts64bin> [options]");
    eprintln!();
    eprintln!("options:");
    eprintln!("  --limit <N>    只取前 N 个点");
    eprintln!("  --stride <S>   每隔 S 个点取一个（默认 1）");
    eprintln!("  --no-stats     不打印 delta 统计");
    eprintln!("  --sort         先排序");
    eprintln!("  --dedup        去掉相邻重复（建议与 --sort 联用）");
    std::process::exit(code)
}
