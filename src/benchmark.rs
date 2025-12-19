// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::Path;

fn not_available() {
    eprintln!("benchmark module is not included in this build");
}

pub fn main() {
    not_available();
}

pub fn main_single_thread() {
    not_available();
}

pub fn main_disk() {
    not_available();
}

pub fn main_disk_single_thread() {
    not_available();
}

pub fn main_real(
    _path: &Path,
    _single_thread: bool,
    _limit: Option<usize>,
    _stride: usize,
    _print_stats: bool,
    _sort: bool,
    _dedup: bool,
) {
    not_available();
}

pub fn main_real_disk(
    _path: &Path,
    _single_thread: bool,
    _limit: Option<usize>,
    _stride: usize,
    _print_stats: bool,
    _sort: bool,
    _dedup: bool,
) {
    not_available();
}
