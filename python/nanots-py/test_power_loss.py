import argparse
import os
import random
import subprocess
import sys
import time

from typing import Optional

import nanots


def run_supervisor(args: argparse.Namespace) -> int:
    path = args.path or "data/power_loss.ntt"
    expected_path = args.expected or f"{path}.expected"

    if args.reset:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        try:
            os.remove(expected_path)
        except FileNotFoundError:
            pass

    rng = random.Random(time.time_ns())
    for round_idx in range(args.rounds):
        cmd = [
            sys.executable,
            os.path.abspath(__file__),
            "--writer",
            "--path",
            path,
            "--expected",
            expected_path,
            "--max-writes",
            str(args.max_writes),
            "--expected-sync-every",
            str(args.expected_sync_every),
            "--max-file-bytes",
            str(args.max_file_bytes),
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        delay_ms = rng.randrange(args.kill_min_ms, max(args.kill_max_ms, args.kill_min_ms) + 1)
        time.sleep(delay_ms / 1000.0)
        proc.kill()
        proc.wait()

        if args.corrupt_bytes > 0 and rng.random() < args.corrupt_prob:
            corrupt_file(path, rng, args.corrupt_bytes)
        verify_db(path, expected_path, args.allow_corruption)
        expected_count = len(read_expected(expected_path))
        print(f"round {round_idx + 1} ok: expected_rows={expected_count}")

    return 0


def run_writer(args: argparse.Namespace) -> int:
    path = args.path or "data/power_loss.ntt"
    expected_path = args.expected or f"{path}.expected"

    db = nanots.Db(path)
    try:
        db.create_table("t", ["v"])
    except Exception:
        pass

    start_seq = (read_last_seq(expected_path) or 0) + 1
    os.makedirs(os.path.dirname(expected_path) or ".", exist_ok=True)
    with open(expected_path, "a", encoding="utf-8") as expected:
        for i in range(args.max_writes):
            if args.max_file_bytes > 0 and os.path.exists(path):
                if os.path.getsize(path) >= args.max_file_bytes:
                    break
            seq = start_seq + i
            db.append_row("t", seq, [float(seq)])
            expected.write(f"{seq}\n")
            if (i + 1) % args.expected_sync_every == 0:
                expected.flush()
                os.fsync(expected.fileno())
    return 0


def verify_db(path: str, expected_path: str, allow_corruption: bool) -> None:
    expected = read_expected(expected_path)
    if not expected:
        return
    max_ts = expected[-1]

    db = nanots.Db(path)
    try:
        ts, cols = db.query_table_range_columns("t", 0, max_ts)
    except Exception:
        if allow_corruption:
            return
        raise
    if len(cols) != 1 or len(ts) != len(cols[0]):
        raise RuntimeError("column length mismatch")

    seen = set()
    for t, v in zip(ts, cols[0]):
        if v != float(t):
            raise RuntimeError("value mismatch")
        seen.add(t)

    for seq in expected:
        if seq not in seen:
            raise RuntimeError("missing expected row")


def read_expected(path: str) -> list[int]:
    if not os.path.exists(path):
        return []
    out: list[int] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            trimmed = line.strip()
            if not trimmed:
                continue
            try:
                out.append(int(trimmed))
            except ValueError:
                pass
    return out


def read_last_seq(path: str) -> Optional[int]:
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        buf = f.read()
    if not buf:
        return None
    pos = len(buf)
    while pos > 0 and buf[pos - 1] == ord("\n"):
        pos -= 1
    while pos > 0 and buf[pos - 1] != ord("\n"):
        pos -= 1
    line = buf[pos:].decode("utf-8", errors="ignore").strip()
    if not line:
        return None
    try:
        return int(line)
    except ValueError:
        return None


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate power loss while writing.")
    parser.add_argument("--writer", action="store_true", help="Run as writer process")
    parser.add_argument("--path", type=str, default=None)
    parser.add_argument("--expected", type=str, default=None)
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--max-writes", type=int, default=100_000)
    parser.add_argument("--kill-min-ms", type=int, default=50)
    parser.add_argument("--kill-max-ms", type=int, default=200)
    parser.add_argument("--expected-sync-every", type=int, default=1)
    parser.add_argument("--max-file-bytes", type=int, default=0)
    parser.add_argument("--corrupt-bytes", type=int, default=0)
    parser.add_argument("--corrupt-prob", type=float, default=0.0)
    parser.add_argument("--allow-corruption", action="store_true")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args(argv)
    if args.expected_sync_every < 1:
        args.expected_sync_every = 1
    if args.corrupt_prob < 0:
        args.corrupt_prob = 0.0
    if args.corrupt_prob > 1:
        args.corrupt_prob = 1.0
    return args


def corrupt_file(path: str, rng: random.Random, bytes_to_flip: int) -> None:
    if not os.path.exists(path):
        return
    size = os.path.getsize(path)
    if size == 0:
        return
    with open(path, "r+b") as f:
        for _ in range(bytes_to_flip):
            offset = rng.randrange(0, size)
            f.seek(offset)
            b = f.read(1)
            if not b:
                continue
            val = b[0]
            bit = 1 << rng.randrange(0, 8)
            f.seek(offset)
            f.write(bytes([val ^ bit]))
        f.flush()
        os.fsync(f.fileno())


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.writer:
        return run_writer(args)
    return run_supervisor(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
