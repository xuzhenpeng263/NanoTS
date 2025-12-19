// SPDX-License-Identifier: AGPL-3.0-or-later
// =========================================================
// Pure Time-Series Compressor (v2.0) - Rust Implementation
// 专注: 时间戳 (Timestamps)
// 核心: Delta-of-Delta (D2) + Head-Body Split + RLE/BitPack/PFOR
// 优化: Buffer Pooling + SIMD + PFOR + Parallel Processing
// =========================================================

use rayon::prelude::*;

const MAGIC: &[u8] = b"TS64";
const BLOCK_SIZE: usize = 1024;

// --- 模式定义 ---
const M_RAW: u8 = 0; // 原始数据 (无法压缩)
const M_RLE: u8 = 1; // 完美规律 (Run-Length)
const M_BP: u8 = 2; // 抖动/正常 (Bit-Packing)
const M_PFOR: u8 = 3; // PFOR (Patched Frame-of-Reference)

// PFOR 参数
#[allow(dead_code)]
const PFOR_THRESHOLD: f32 = 0.9; // 90% 的数能用较少位数表示

// =========================================================
// Buffer Pool - 内存池，避免重复分配
// =========================================================
struct BufferPool {
    d2_buffer: Vec<i64>,
    z_body_buffer: Vec<u64>,
    exceptions: Vec<(usize, i64)>, // PFOR 异常值: (index, i64 value in D2 domain)
    exceptions_bytes: Vec<u8>,
    compressed_buffer: Vec<u8>,
}

impl BufferPool {
    fn new() -> Self {
        Self {
            d2_buffer: Vec::with_capacity(BLOCK_SIZE),
            z_body_buffer: Vec::with_capacity(BLOCK_SIZE),
            exceptions: Vec::new(),
            exceptions_bytes: Vec::new(),
            compressed_buffer: Vec::with_capacity(BLOCK_SIZE * 8),
        }
    }

    fn clear(&mut self) {
        self.d2_buffer.clear();
        self.z_body_buffer.clear();
        self.exceptions.clear();
        self.exceptions_bytes.clear();
        self.compressed_buffer.clear();
    }
}

// =========================================================
// Bit Writer - 按位写入 (保留作为备用)
// =========================================================
#[allow(dead_code)]
struct BitWriter {
    buf: Vec<u8>,
    acc: u128,
    nbits: u32,
}

#[allow(dead_code)]
impl BitWriter {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            acc: 0,
            nbits: 0,
        }
    }

    fn write_bits(&mut self, v: u64, n: u8) {
        if n == 0 {
            return;
        }
        let n = n as u32;
        let mask: u128 = if n == 64 { (1u128 << 64) - 1 } else { (1u128 << n) - 1 };
        let v = (v as u128) & mask;
        self.acc |= v << self.nbits;
        self.nbits += n;

        while self.nbits >= 8 {
            self.buf.push((self.acc & 0xFF) as u8);
            self.acc >>= 8;
            self.nbits -= 8;
        }
    }

    fn get_bytes(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            self.buf.push((self.acc & 0xFF) as u8);
        }
        self.buf
    }
}

// =========================================================
// Bit Reader - 按位读取
// =========================================================
#[allow(dead_code)]
struct BitReader<'a> {
    buf: &'a [u8],
    pos: usize,
    acc: u128,
    nbits: u32,
}

#[allow(dead_code)]
impl<'a> BitReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            acc: 0,
            nbits: 0,
        }
    }

    fn read_bits(&mut self, n: u8) -> Result<u64, &'static str> {
        if n == 0 {
            return Ok(0);
        }
        if n > 64 {
            return Err("BitReader: n > 64");
        }
        let n = n as u32;

        while self.nbits < n {
            if self.pos >= self.buf.len() {
                return Err("BitReader: EOF");
            }
            self.acc |= (self.buf[self.pos] as u128) << self.nbits;
            self.nbits += 8;
            self.pos += 1;
        }

        let mask: u128 = if n == 64 { (1u128 << 64) - 1 } else { (1u128 << n) - 1 };
        let v = (self.acc & mask) as u64;
        self.acc >>= n;
        self.nbits -= n;

        Ok(v)
    }
}

// =========================================================
// Variable Length Integer (varint) - 与 Python 保持一致
// =========================================================

fn uvarint_encode(mut x: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut b = (x & 0x7F) as u8;
        x >>= 7;
        if x != 0 {
            b |= 0x80;
            out.push(b);
        } else {
            out.push(b);
            break;
        }
    }
    out
}

fn uvarint_decode(buf: &[u8], pos: usize) -> Result<(u64, usize), &'static str> {
    let mut val = 0u64;
    let mut shift = 0;
    let mut pos = pos;

    loop {
        if pos >= buf.len() {
            return Err("uvarint_decode: EOF");
        }
        let b = buf[pos];
        pos += 1;
        val |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            return Ok((val, pos));
        }
        shift += 7;
    }
}

#[inline]
fn uvarint_len(mut x: u64) -> usize {
    let mut n = 1usize;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}

// =========================================================
// ZigZag Encoding - 64位
// =========================================================

fn zz_encode_scalar(x: i64) -> u64 {
    ((x << 1) ^ (x >> 63)) as u64
}

fn zz_decode_scalar(u: u64) -> i64 {
    let signed = (u >> 1) as i64;
    signed ^ -((u & 1) as i64)
}

// =========================================================
// 使用自定义的 SIMD 位打包函数 (兼容 u64)
// =========================================================
#[inline]
fn packed_bytes_len(n: usize, bit_width: u8) -> usize {
    let total_bits = (n as u64) * (bit_width as u64);
    ((total_bits + 7) / 8) as usize
}

#[inline]
unsafe fn write_u64_le(dst: *mut u8, v: u64) {
    std::ptr::write_unaligned(dst as *mut u64, u64::to_le(v));
}

#[inline]
unsafe fn write_u64_tail_le(dst: *mut u8, v: u64, nbytes: usize) {
    debug_assert!(nbytes <= 8);
    let bytes = u64::to_le_bytes(v);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, nbytes);
}

#[inline]
unsafe fn read_u64_le(src: *const u8) -> u64 {
    u64::from_le(std::ptr::read_unaligned(src as *const u64))
}

fn simd_bitpack_into(input: &[u64], bit_width: u8, out: &mut Vec<u8>) {
    out.clear();
    if bit_width == 0 || input.is_empty() {
        return;
    }

    let bytes_len = packed_bytes_len(input.len(), bit_width);
    out.resize(bytes_len, 0);

    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    let mut out_pos: usize = 0;
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };

    let out_ptr = out.as_mut_ptr();
    for &raw_v in input {
        let v = raw_v & mask;
        acc |= (v as u128) << acc_bits;
        acc_bits += bit_width as u32;

        while acc_bits >= 64 {
            let word = (acc & (u64::MAX as u128)) as u64;
            unsafe { write_u64_le(out_ptr.add(out_pos), word) };
            out_pos += 8;
            acc >>= 64;
            acc_bits -= 64;
        }
    }

    if acc_bits > 0 && out_pos < bytes_len {
        let word = (acc & (u64::MAX as u128)) as u64;
        let remaining = bytes_len - out_pos;
        unsafe {
            if remaining >= 8 {
                write_u64_le(out_ptr.add(out_pos), word);
            } else {
                write_u64_tail_le(out_ptr.add(out_pos), word, remaining);
            }
        };
    }
}

#[inline]
fn simd_bitunpack_into(input: &[u8], bit_width: u8, out: &mut [u64]) {
    if bit_width == 0 || out.is_empty() {
        out.fill(0);
        return;
    }

    let n = out.len();
    let bytes_len = packed_bytes_len(n, bit_width);
    let input = if input.len() >= bytes_len {
        &input[..bytes_len]
    } else {
        input
    };

    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    let mut in_pos: usize = 0;
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };

    let in_ptr = input.as_ptr();
    for slot in out.iter_mut() {
        while acc_bits < bit_width as u32 {
            let remaining = input.len().saturating_sub(in_pos);
            let word = if remaining >= 8 {
                unsafe { read_u64_le(in_ptr.add(in_pos)) }
            } else if remaining > 0 {
                let mut tmp = [0u8; 8];
                unsafe {
                    std::ptr::copy_nonoverlapping(in_ptr.add(in_pos), tmp.as_mut_ptr(), remaining)
                };
                u64::from_le_bytes(tmp)
            } else {
                0u64
            };

            acc |= (word as u128) << acc_bits;
            acc_bits += 64;
            in_pos += 8;
        }

        *slot = (acc as u64) & mask;
        acc >>= bit_width as u32;
        acc_bits -= bit_width as u32;
    }
}

#[inline]
unsafe fn read_u64_be(src: *const u8) -> u64 {
    u64::from_be(std::ptr::read_unaligned(src as *const u64))
}

struct DecompressScratch {
    unpacked_u64: Vec<u64>,
    exceptions: Vec<(usize, i64)>,
}

impl DecompressScratch {
    fn new() -> Self {
        Self {
            unpacked_u64: Vec::new(),
            exceptions: Vec::new(),
        }
    }
}

struct BlockMeta {
    mode: u8,
    param: u8,
    payload_start: usize,
    payload_end: usize,
    block_size: usize,
}

fn decompress_block_into(
    payload: &[u8],
    mode: u8,
    param: u8,
    out: &mut [i64],
    scratch: &mut DecompressScratch,
) -> Result<(), &'static str> {
    if out.is_empty() {
        return Ok(());
    }

    let mut pos = 0usize;
    let block_end = payload.len();

    let (v1, new_pos) = uvarint_decode(payload, pos)?;
    pos = new_pos;
    let ref_val = zz_decode_scalar(v1);
    out[0] = ref_val;

    if out.len() == 1 {
        return Ok(());
    }

    if pos >= block_end {
        return Err("Header incomplete");
    }
    let (v2, new_pos) = uvarint_decode(payload, pos)?;
    pos = new_pos;
    let d0 = zz_decode_scalar(v2);

    out[1] = ref_val.wrapping_add(d0);

    let body_len = out.len() - 2;
    if body_len == 0 {
        return Ok(());
    }

    let mut d1 = d0;
    let mut ts = out[1];

    if mode == M_RLE {
        for slot in &mut out[2..] {
            ts = ts.wrapping_add(d0);
            *slot = ts;
        }
        return Ok(());
    }

    if mode == M_BP {
        scratch.unpacked_u64.resize(body_len, 0);
        simd_bitunpack_into(
            &payload[pos..block_end],
            param,
            scratch.unpacked_u64.as_mut_slice(),
        );
        for (slot, &z) in out[2..].iter_mut().zip(scratch.unpacked_u64.iter()) {
            let d2 = zz_decode_scalar(z);
            d1 = d1.wrapping_add(d2);
            ts = ts.wrapping_add(d1);
            *slot = ts;
        }
        return Ok(());
    }

    if mode == M_PFOR {
        let (exception_count_u64, mut cursor) = uvarint_decode(payload, pos)?;
        let exception_count = exception_count_u64 as usize;

        scratch.exceptions.clear();
        scratch.exceptions.reserve(exception_count);

        let mut idx_acc = 0usize;
        let mut prev_val = 0i64;
        for i in 0..exception_count {
            let (idx_part_u64, new_pos) = uvarint_decode(payload, cursor)?;
            cursor = new_pos;

            let idx_part = idx_part_u64 as usize;
            idx_acc = if i == 0 { idx_part } else { idx_acc + idx_part };

            let (val_part_u64, new_pos) = uvarint_decode(payload, cursor)?;
            cursor = new_pos;

            let delta = zz_decode_scalar(val_part_u64);
            let val_i64 = if i == 0 {
                delta
            } else {
                prev_val.wrapping_add(delta)
            };
            prev_val = val_i64;

            scratch.exceptions.push((idx_acc, val_i64));
        }

        if cursor > block_end {
            return Err("PFOR exceptions exceed payload");
        }

        scratch.unpacked_u64.resize(body_len, 0);
        simd_bitunpack_into(
            &payload[cursor..block_end],
            param,
            scratch.unpacked_u64.as_mut_slice(),
        );

        for &(idx, val_i64) in &scratch.exceptions {
            if idx >= body_len {
                return Err("PFOR exception index out of range");
            }
            scratch.unpacked_u64[idx] = zz_encode_scalar(val_i64);
        }

        for (slot, &z) in out[2..].iter_mut().zip(scratch.unpacked_u64.iter()) {
            let d2 = zz_decode_scalar(z);
            d1 = d1.wrapping_add(d2);
            ts = ts.wrapping_add(d1);
            *slot = ts;
        }
        return Ok(());
    }

    if mode == M_RAW {
        let raw_data = &payload[pos..block_end];
        if raw_data.len() != body_len * 8 {
            return Err("Raw body size mismatch");
        }

        unsafe {
            let ptr = raw_data.as_ptr();
            for (i, slot) in out[2..].iter_mut().enumerate() {
                let z_val = read_u64_be(ptr.add(i * 8));
                let d2 = zz_decode_scalar(z_val);
                d1 = d1.wrapping_add(d2);
                ts = ts.wrapping_add(d1);
                *slot = ts;
            }
        }

        return Ok(());
    }

    Err("Unknown mode")
}

#[inline]
fn bit_width_u64(x: u64) -> u8 {
    if x == 0 {
        0
    } else {
        64 - x.leading_zeros() as u8
    }
}

#[inline]
fn build_bit_width_hist(values: &[u64]) -> [usize; 65] {
    let mut hist = [0usize; 65]; // bit width 0..=64
    for &v in values {
        hist[bit_width_u64(v) as usize] += 1;
    }
    hist
}

#[inline]
fn bit_width_for_threshold(hist: &[usize; 65], len: usize, threshold: f32) -> u8 {
    debug_assert!((0.0..=1.0).contains(&threshold));
    if len == 0 {
        return 0;
    }

    let target = ((len as f32) * threshold).ceil() as usize;
    let mut cumsum = 0usize;
    for (bw, &cnt) in hist.iter().enumerate() {
        cumsum += cnt;
        if cumsum >= target {
            return bw as u8;
        }
    }
    64
}

#[inline]
fn encode_pfor_exceptions(exceptions: &[(usize, i64)], out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(&uvarint_encode(exceptions.len() as u64));
    if exceptions.is_empty() {
        return;
    }

    let mut prev_idx = 0usize;
    let mut prev_val = 0i64;

    for (i, &(idx, val)) in exceptions.iter().enumerate() {
        let idx_to_write = if i == 0 { idx } else { idx - prev_idx };
        out.extend_from_slice(&uvarint_encode(idx_to_write as u64));

        let delta = if i == 0 {
            val
        } else {
            val.wrapping_sub(prev_val)
        };
        out.extend_from_slice(&uvarint_encode(zz_encode_scalar(delta)));

        prev_idx = idx;
        prev_val = val;
    }
}

#[inline]
fn estimate_pfor_total_len(values: &[u64], bw_pfor: u8, head_len: usize) -> Option<(usize, usize)> {
    if values.is_empty() {
        return None;
    }

    let mut exception_count = 0usize;
    let mut exceptions_len = 0usize; // bytes for exception_count + encoded exceptions
    let mut prev_idx = 0usize;
    let mut prev_val = 0i64;

    for (idx, &v) in values.iter().enumerate() {
        if bit_width_u64(v) <= bw_pfor {
            continue;
        }
        let val_i64 = zz_decode_scalar(v);
        if exception_count == 0 {
            // first exception: absolute idx, absolute val
            exceptions_len += uvarint_len(idx as u64);
            exceptions_len += uvarint_len(zz_encode_scalar(val_i64));
        } else {
            // subsequent: delta idx, delta val
            exceptions_len += uvarint_len((idx - prev_idx) as u64);
            exceptions_len += uvarint_len(zz_encode_scalar(val_i64.wrapping_sub(prev_val)));
        }

        prev_idx = idx;
        prev_val = val_i64;
        exception_count += 1;
    }

    if exception_count == 0 {
        return None;
    }

    // exception_count itself
    exceptions_len += uvarint_len(exception_count as u64);

    let packed_len = packed_bytes_len(values.len(), bw_pfor);
    let payload_len = head_len + exceptions_len + packed_len;
    let total_len = 1 + 1 + uvarint_len(payload_len as u64) + payload_len;
    Some((total_len, exception_count))
}

// =========================================================
// 单块压缩函数 - 用于并行处理
// =========================================================
fn compress_block(timestamps: &[i64], pool: &mut BufferPool) -> Vec<u8> {
    pool.clear();
    let n_sub = timestamps.len();

    // --- 核心: 计算 Delta-of-Delta (D2) ---
    pool.d2_buffer.resize(n_sub, 0);
    pool.d2_buffer[0] = timestamps[0]; // Ref (Head)

    if n_sub > 1 {
        pool.d2_buffer[1] = timestamps[1] - timestamps[0]; // First Delta (Head)
        if n_sub > 2 {
            compute_d2_body(&timestamps[..n_sub], &mut pool.d2_buffer[..n_sub]);
        }
    }

    // --- Head-Body Split ---

    // 1. 编码 Header
    let mut head_bytes = Vec::new();
    head_bytes.extend_from_slice(&uvarint_encode(zz_encode_scalar(pool.d2_buffer[0])));
    if n_sub > 1 {
        head_bytes.extend_from_slice(&uvarint_encode(zz_encode_scalar(pool.d2_buffer[1])));
    }

    // 2. 编码 Body
    if n_sub <= 2 {
        // 只有头，没有身子
        let mut out = Vec::with_capacity(1 + head_bytes.len());
        out.push(M_RAW);
        out.extend_from_slice(&uvarint_encode(head_bytes.len() as u64));
        out.extend_from_slice(&head_bytes);
        return out;
    }

    let body = &pool.d2_buffer[2..];

    // 策略 A: RLE (检查是否全为 0)
    if body.iter().all(|&x| x == 0) {
        let mut out = Vec::with_capacity(1 + head_bytes.len());
        out.push(M_RLE);
        out.extend_from_slice(&uvarint_encode(head_bytes.len() as u64));
        out.extend_from_slice(&head_bytes);
        return out;
    }

    // ZigZag 编码 body
    pool.z_body_buffer.resize(body.len(), 0);
    let mut max_val = 0u64;
    unsafe {
        let src_ptr = body.as_ptr();
        let dst_ptr = pool.z_body_buffer.as_mut_ptr();
        for i in 0..body.len() {
            let z = zz_encode_scalar(*src_ptr.add(i));
            *dst_ptr.add(i) = z;
            max_val = max_val.max(z);
        }
    }

    // BitPacking 的基准位宽（最大值决定）
    let bw_max = bit_width_u64(max_val);

    // 策略 C: BitPacking (使用优化的位打包)
    let mut out = Vec::new();

    // 如果位宽太大 (>60)，说明数据极其混乱，直接 Raw
    if bw_max > 60 {
        out.push(M_RAW);
        // Raw 模式下，Body 直接用 bytes
        let mut raw_body = Vec::with_capacity(pool.z_body_buffer.len() * 8);
        for &x in &pool.z_body_buffer {
            raw_body.extend_from_slice(&x.to_be_bytes());
        }
        let total_payload = [head_bytes.as_slice(), raw_body.as_slice()].concat();
        out.extend_from_slice(&uvarint_encode(total_payload.len() as u64));
        out.extend_from_slice(&total_payload);
    } else {
        // 候选 1) 纯 BitPack
        let bp_packed_len = packed_bytes_len(pool.z_body_buffer.len(), bw_max);
        let bp_payload_len = head_bytes.len() + bp_packed_len;
        let bp_total_len = 1 + 1 + uvarint_len(bp_payload_len as u64) + bp_payload_len;

        // 候选 2) PFOR（剥离异常值） - 选择更优的位宽候选，避免少量异常值把整块 bw 拉高
        let mut best_pfor_total_len: Option<usize> = None;
        let mut best_pfor_bw: u8 = 0;

        let hist = build_bit_width_hist(&pool.z_body_buffer);
        let mut candidates: [u8; 12] = [0; 12];
        let mut cand_len = 0usize;

        // 经验分位点 + 极端值候选（去重后再评估真实 total_len）
        for thr in [0.50f32, 0.60, 0.70, 0.80, 0.85, 0.90, 0.95, 0.97, 0.99] {
            let bw = bit_width_for_threshold(&hist, pool.z_body_buffer.len(), thr);
            if bw < bw_max {
                candidates[cand_len] = bw;
                cand_len += 1;
            }
        }
        if bw_max > 0 {
            candidates[cand_len] = bw_max - 1;
            cand_len += 1;
        }

        // 去重（cand_len 很小，用 O(n^2) 足够）
        for i in 0..cand_len {
            let bw_pfor = candidates[i];
            if (0..i).any(|j| candidates[j] == bw_pfor) {
                continue;
            }
            if bw_pfor >= bw_max {
                continue;
            }

            if let Some((total_len, _exception_count)) =
                estimate_pfor_total_len(&pool.z_body_buffer, bw_pfor, head_bytes.len())
            {
                match best_pfor_total_len {
                    None => {
                        best_pfor_total_len = Some(total_len);
                        best_pfor_bw = bw_pfor;
                    }
                    Some(best) if total_len < best => {
                        best_pfor_total_len = Some(total_len);
                        best_pfor_bw = bw_pfor;
                    }
                    _ => {}
                }
            }
        }

        let use_pfor = match best_pfor_total_len {
            Some(pfor_total) => pfor_total < bp_total_len,
            None => false,
        };

        if use_pfor {
            // 编码 PFOR:
            // [mode=PFOR][param=bw_pfor][payload_len][head...][exception_count][(idx,val)...][packed_normals...]
            out.push(M_PFOR);
            out.push(best_pfor_bw);

            // 收集异常值（值存 D2 域 i64），并把 normals 中的异常位清零
            pool.exceptions.clear();
            for (idx, &v) in pool.z_body_buffer.iter().enumerate() {
                if bit_width_u64(v) > best_pfor_bw {
                    pool.exceptions.push((idx, zz_decode_scalar(v)));
                }
            }

            encode_pfor_exceptions(&pool.exceptions, &mut pool.exceptions_bytes);

            // normals：异常位置置 0（占位）
            for &(idx, _) in &pool.exceptions {
                pool.z_body_buffer[idx] = 0;
            }
            simd_bitpack_into(
                &pool.z_body_buffer,
                best_pfor_bw,
                &mut pool.compressed_buffer,
            );

            let total_len =
                head_bytes.len() + pool.exceptions_bytes.len() + pool.compressed_buffer.len();
            out.extend_from_slice(&uvarint_encode(total_len as u64));
            out.extend_from_slice(&head_bytes);
            out.extend_from_slice(&pool.exceptions_bytes);
            out.extend_from_slice(&pool.compressed_buffer);
        } else {
            // 使用优化的 BitPack
            out.push(M_BP);
            out.push(bw_max); // 写入位宽 param

            simd_bitpack_into(&pool.z_body_buffer, bw_max, &mut pool.compressed_buffer);

            // 写入
            let total_len = head_bytes.len() + pool.compressed_buffer.len();
            out.extend_from_slice(&uvarint_encode(total_len as u64));
            out.extend_from_slice(&head_bytes);
            out.extend_from_slice(&pool.compressed_buffer);
        }
    }

    out
}

#[inline]
fn compute_d2_body(timestamps: &[i64], d2_out: &mut [i64]) {
    debug_assert_eq!(timestamps.len(), d2_out.len());
    if timestamps.len() <= 2 {
        return;
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe { return compute_d2_body_avx2(timestamps, d2_out) };
        }
    }

    // 标量回退：d2[i] = ts[i] - 2*ts[i-1] + ts[i-2]
    unsafe {
        let ts = timestamps.as_ptr();
        let out = d2_out.as_mut_ptr();
        let n = timestamps.len();
        for i in 2..n {
            let a = *ts.add(i);
            let b = *ts.add(i - 1);
            let c = *ts.add(i - 2);
            *out.add(i) = a.wrapping_sub(b.wrapping_mul(2)).wrapping_add(c);
        }
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn compute_d2_body_avx2(timestamps: &[i64], d2_out: &mut [i64]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let n = timestamps.len();
    if n <= 2 {
        return;
    }

    let ts_ptr = timestamps.as_ptr();
    let out_ptr = d2_out.as_mut_ptr();

    let mut i = 2usize;
    while i + 4 <= n {
        let a = _mm256_loadu_si256(ts_ptr.add(i - 2) as *const __m256i); // ts[i-2..i+2)
        let b = _mm256_loadu_si256(ts_ptr.add(i - 1) as *const __m256i); // ts[i-1..i+3)
        let c = _mm256_loadu_si256(ts_ptr.add(i) as *const __m256i); // ts[i..i+4)

        let doubled_b = _mm256_slli_epi64(b, 1);
        let tmp = _mm256_sub_epi64(c, doubled_b);
        let res = _mm256_add_epi64(tmp, a);
        _mm256_storeu_si256(out_ptr.add(i) as *mut __m256i, res);

        i += 4;
    }

    for idx in i..n {
        let a = *ts_ptr.add(idx);
        let b = *ts_ptr.add(idx - 1);
        let c = *ts_ptr.add(idx - 2);
        *out_ptr.add(idx) = a.wrapping_sub(b.wrapping_mul(2)).wrapping_add(c);
    }
}

// =========================================================
// 时序压缩引擎
// =========================================================

pub struct TimeSeriesCompressor;

impl TimeSeriesCompressor {
    /// 压缩时间戳序列 - 多线程版本
    pub fn compress(timestamps: &[i64]) -> Vec<u8> {
        let n = timestamps.len();

        // 空数据处理
        if n == 0 {
            let mut out = Vec::with_capacity(8);
            out.extend_from_slice(MAGIC);
            out.extend_from_slice(&0u32.to_be_bytes());
            return out;
        }

        let mut out = Vec::new();

        // Header: MAGIC + Total Count
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(n as u32).to_be_bytes());

        // 并行处理块
        let chunks: Vec<_> = timestamps.chunks(BLOCK_SIZE).collect();
        let compressed_blocks: Vec<Vec<u8>> = chunks
            .par_iter()
            .map(|chunk| {
                let mut pool = BufferPool::new();
                compress_block(chunk, &mut pool)
            })
            .collect();

        // 合并所有压缩块
        for block in compressed_blocks {
            out.extend_from_slice(&block);
        }

        out
    }

    /// 压缩时间戳序列 - 单线程版本（用于对比）
    pub fn compress_single_thread(timestamps: &[i64]) -> Vec<u8> {
        let n = timestamps.len();

        // 空数据处理
        if n == 0 {
            let mut out = Vec::with_capacity(8);
            out.extend_from_slice(MAGIC);
            out.extend_from_slice(&0u32.to_be_bytes());
            return out;
        }

        let mut out = Vec::new();
        let mut pool = BufferPool::new();

        // Header: MAGIC + Total Count
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(n as u32).to_be_bytes());

        // Block Processing
        for base in (0..n).step_by(BLOCK_SIZE) {
            let end = (base + BLOCK_SIZE).min(n);
            let sub = &timestamps[base..end];

            let block_data = compress_block(sub, &mut pool);
            out.extend_from_slice(&block_data);
        }

        out
    }

    /// 解压时间戳序列
    pub fn decompress(blob: &[u8]) -> Result<Vec<i64>, &'static str> {
        if blob.len() < 8 || !blob.starts_with(MAGIC) {
            return Err("Invalid format");
        }

        // 读取总数
        let n = u32::from_be_bytes([blob[4], blob[5], blob[6], blob[7]]) as usize;
        if n == 0 {
            return Ok(Vec::new());
        }

        let mut out_arr = vec![0i64; n];

        let mut pos = 8usize;
        let mut remaining = n;
        let mut blocks = Vec::with_capacity((n + BLOCK_SIZE - 1) / BLOCK_SIZE);

        while remaining > 0 {
            let block_size = BLOCK_SIZE.min(remaining);

            if pos >= blob.len() {
                return Err("Unexpected EOF");
            }
            let mode = blob[pos];
            pos += 1;

            let param = if mode == M_BP || mode == M_PFOR {
                if pos >= blob.len() {
                    return Err("Unexpected EOF");
                }
                let p = blob[pos];
                pos += 1;
                p
            } else {
                0
            };

            let (plen, new_pos) = uvarint_decode(blob, pos)?;
            pos = new_pos;

            let plen_usize = plen as usize;
            if pos + plen_usize > blob.len() {
                return Err("Payload exceeds blob size");
            }
            let payload_start = pos;
            let payload_end = pos + plen_usize;

            blocks.push(BlockMeta {
                mode,
                param,
                payload_start,
                payload_end,
                block_size,
            });

            pos = payload_end;
            remaining -= block_size;
        }

        let use_parallel = n >= BLOCK_SIZE * 4 && rayon::current_num_threads() > 1;
        if use_parallel {
            out_arr
                .par_chunks_mut(BLOCK_SIZE)
                .zip(blocks.par_iter())
                .try_for_each_init(DecompressScratch::new, |scratch, (chunk, meta)| {
                    decompress_block_into(
                        &blob[meta.payload_start..meta.payload_end],
                        meta.mode,
                        meta.param,
                        &mut chunk[..meta.block_size],
                        scratch,
                    )
                })?;
        } else {
            let mut scratch = DecompressScratch::new();
            for (chunk, meta) in out_arr.chunks_mut(BLOCK_SIZE).zip(blocks.iter()) {
                decompress_block_into(
                    &blob[meta.payload_start..meta.payload_end],
                    meta.mode,
                    meta.param,
                    &mut chunk[..meta.block_size],
                    &mut scratch,
                )?;
            }
        }

        Ok(out_arr)
    }
}

pub struct FloatXorCompressor;

impl FloatXorCompressor {
    pub fn compress(values: &[f64]) -> Vec<u8> {
        compress_f64_xor(values)
    }

    pub fn decompress(blob: &[u8], count: usize) -> Result<Vec<f64>, &'static str> {
        decompress_f64_xor(blob, count)
    }
}

pub fn compress_f64_xor(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut writer = BitWriter::new();
    let mut prev = canonicalize_nan(values[0]).to_bits();
    writer.write_bits(prev, 64);

    let mut prev_leading: u8 = 0;
    let mut prev_trailing: u8 = 0;
    let mut prev_sig: u8 = 0;
    let mut has_prev_window = false;

    for &v in &values[1..] {
        let curr = canonicalize_nan(v).to_bits();
        let xor = prev ^ curr;
        if xor == 0 {
            writer.write_bits(0, 1);
        } else {
            writer.write_bits(1, 1);

            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;
            let sig = 64u8 - leading - trailing;

            if has_prev_window && leading >= prev_leading && trailing >= prev_trailing {
                writer.write_bits(0, 1);
                let x = xor >> prev_trailing;
                writer.write_bits(x, prev_sig);
            } else {
                writer.write_bits(1, 1);
                // Store leading (0..63) and significant bits length (1..64) in 6 bits each.
                writer.write_bits(leading as u64, 6);
                writer.write_bits(sig as u64, 6);
                let x = xor >> trailing;
                writer.write_bits(x, sig);
                prev_leading = leading;
                prev_trailing = trailing;
                prev_sig = sig;
                has_prev_window = true;
            }
        }
        prev = curr;
    }

    writer.get_bytes()
}

fn canonicalize_nan(v: f64) -> f64 {
    if v.is_nan() {
        // Canonical quiet NaN payload to improve compression and stable bit-roundtrips.
        return f64::from_bits(0x7ff8_0000_0000_0000);
    }
    v
}

pub fn decompress_f64_xor(blob: &[u8], count: usize) -> Result<Vec<f64>, &'static str> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut reader = BitReader::new(blob);

    let first = reader.read_bits(64)?;
    let mut out = Vec::with_capacity(count);
    out.push(f64::from_bits(first));

    let mut prev = first;
    let mut prev_trailing: u8 = 0;
    let mut prev_sig: u8 = 0;
    let mut has_prev_window = false;

    while out.len() < count {
        let control = reader.read_bits(1)?;
        if control == 0 {
            out.push(f64::from_bits(prev));
            continue;
        }

        let sub = reader.read_bits(1)?;
        let xor = if sub == 0 {
            if !has_prev_window {
                return Err("xor stream missing window");
            }
            let x = reader.read_bits(prev_sig)?;
            x << prev_trailing
        } else {
            let leading = reader.read_bits(6)? as u8;
            let sig = reader.read_bits(6)? as u8;
            if sig == 0 || sig > 64 {
                return Err("invalid sigbits");
            }
            let trailing = 64u8 - leading - sig;
            let x = reader.read_bits(sig)?;
            has_prev_window = true;
            prev_trailing = trailing;
            prev_sig = sig;
            x << trailing
        };

        prev ^= xor;
        out.push(f64::from_bits(prev));
    }

    Ok(out)
}

// =========================================================
// 测试模块
// =========================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_data() {
        let data = vec![];
        let compressed = TimeSeriesCompressor::compress(&data);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_single_timestamp() {
        let data = vec![1704067200000i64];
        let compressed = TimeSeriesCompressor::compress(&data);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_perfect_regular() {
        let mut data = Vec::new();
        let start = 1704067200000i64;
        for i in 0..1000 {
            data.push(start + (i * 1000) as i64);
        }

        let compressed = TimeSeriesCompressor::compress(&data);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_with_jitter() {
        let mut data = Vec::new();
        let start = 1704067200000i64;
        for i in 0..1000 {
            let jitter = if i % 2 == 0 { 10 } else { -10 };
            data.push(start + (i * 1000) as i64 + jitter);
        }

        let compressed = TimeSeriesCompressor::compress(&data);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_large_values() {
        let mut data = Vec::new();
        let start = 1704067200000i64;
        for i in 0..100 {
            data.push(start + (i as i64).wrapping_mul(86400000)); // 每天一个点
        }

        let compressed = TimeSeriesCompressor::compress(&data);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_random_jitter() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut data = Vec::new();
        let start = 1704067200000i64;
        let mut hasher = DefaultHasher::new();

        for i in 0..10000 {
            i.hash(&mut hasher);
            let jitter = (hasher.finish() % 101) as i64 - 50; // -50 到 50
            data.push(start + (i * 1000) as i64 + jitter);
        }

        let compressed = TimeSeriesCompressor::compress(&data);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_block_boundaries() {
        // 测试跨块边界的情况
        let mut data = Vec::new();
        let start = 1704067200000i64;

        // 创建 3个块的数据 (3 * BLOCK_SIZE)
        for i in 0..(BLOCK_SIZE * 3) {
            data.push(start + (i * 1000) as i64);
        }

        // 在边界处添加抖动
        data[BLOCK_SIZE - 1] += 100;
        data[BLOCK_SIZE] -= 50;
        data[BLOCK_SIZE * 2 - 1] += 75;
        data[BLOCK_SIZE * 2] -= 25;

        let compressed = TimeSeriesCompressor::compress(&data);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_pfor_compatibility() {
        // 测试有异常值的数据
        let mut data = Vec::new();
        let start = 1704067200000i64;

        for i in 0..1024 {
            let base = start + (i * 1000) as i64;
            let jitter = if i == 512 {
                // 一个大的异常值
                1000000
            } else {
                // 其他都是小抖动
                ((i % 10) as i64 - 5) * 10
            };
            data.push(base + jitter);
        }

        let compressed = TimeSeriesCompressor::compress(&data);
        assert_eq!(compressed[8], M_PFOR);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_pfor_threshold_selection_more_robust() {
        // 构造“绝大多数很小，少量很大”的 D2 分布：
        // 如果只用少数阈值候选，容易选不到更优 bw_pfor，导致整块被高 bw 的 BP 拉大。
        let start = 1704067200000i64;
        let mut data = Vec::with_capacity(BLOCK_SIZE);
        let mut ts = start;
        data.push(ts);

        // base: 1s 规律
        for i in 1..BLOCK_SIZE {
            ts += 1000;
            // 少量位置注入大跳变（制造高 bw_max）
            if i % 127 == 0 {
                ts += 5_000_000;
            }
            data.push(ts);
        }

        let compressed = TimeSeriesCompressor::compress(&data);
        // 该用例期望能触发 PFOR（即便具体 bw 取决于实现细节）
        assert_eq!(compressed[8], M_PFOR);
        let decompressed = TimeSeriesCompressor::decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_single_thread_vs_multi_thread() {
        // 测试单线程和多线程版本的一致性
        let mut data = Vec::new();
        let start = 1704067200000i64;
        for i in 0..5000 {
            let jitter = ((i * 37) % 100 - 50) as i64;
            data.push(start + (i * 1000) as i64 + jitter);
        }

        let compressed_single = TimeSeriesCompressor::compress_single_thread(&data);
        let compressed_multi = TimeSeriesCompressor::compress(&data);

        let decompressed_single = TimeSeriesCompressor::decompress(&compressed_single).unwrap();
        let decompressed_multi = TimeSeriesCompressor::decompress(&compressed_multi).unwrap();

        assert_eq!(data, decompressed_single);
        assert_eq!(data, decompressed_multi);
        assert_eq!(decompressed_single, decompressed_multi);
    }

    #[test]
    fn test_f64_xor_roundtrip_bits() {
        let mut values = Vec::new();
        // Deterministic float-ish sequence with repeats.
        let mut x = 0.0f64;
        for i in 0..10_000usize {
            if i % 7 == 0 {
                values.push(x);
                continue;
            }
            x += ((i % 101) as f64) * 0.001;
            values.push(x);
        }

        let blob = compress_f64_xor(&values);
        let out = decompress_f64_xor(&blob, values.len()).unwrap();
        assert_eq!(values.len(), out.len());
        for (a, b) in values.iter().zip(out.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
}
