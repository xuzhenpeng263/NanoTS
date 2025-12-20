#![no_main]

use libfuzzer_sys::fuzz_target;
use nanots::compressor::{compress_f64_xor, decompress_f64_xor, TimeSeriesCompressor};

fuzz_target!(|data: &[u8]| {
    let _ = TimeSeriesCompressor::decompress(data);

    let mut ts = Vec::with_capacity(data.len() / 8);
    for chunk in data.chunks_exact(8) {
        let val = i64::from_le_bytes(chunk.try_into().unwrap());
        ts.push(val);
    }
    let blob = TimeSeriesCompressor::compress(&ts);
    let _ = TimeSeriesCompressor::decompress(&blob);

    let mut values = Vec::with_capacity(data.len() / 8);
    for chunk in data.chunks_exact(8) {
        let val = f64::from_bits(u64::from_le_bytes(chunk.try_into().unwrap()));
        values.push(val);
    }
    let encoded = compress_f64_xor(&values);
    let _ = decompress_f64_xor(&encoded, values.len());
    let count = data.len().min(8192) / 8;
    let _ = decompress_f64_xor(data, count);
});
