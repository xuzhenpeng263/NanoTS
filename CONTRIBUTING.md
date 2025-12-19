# Contributing

Thanks for your interest in contributing to NanoTS (Community Edition). This document explains how to propose changes, set up a dev environment, and submit high-quality pull requests.

## License and CLA

- License: AGPL-3.0-or-later
- All contributions are accepted under the project license.
- You must agree to the CLA before code contributions. See `CLA.md`.

## Ways to contribute

- Bug reports and reproducible test cases
- Documentation improvements (README, SPEC, API docs)
- Performance work (benchmarks, profiling notes)
- New features that match the project scope

If you are unsure, open an issue first with your proposal and expected use cases.

## Development setup

### Prerequisites

- Rust toolchain (stable) with `cargo`
- Optional: `maturin` and Python 3.8+ for the Python bindings

### Build and test

```bash
cargo build
cargo test
```

### Run benchmarks

```bash
cargo run --release -- benchmark
```

### Python bindings (optional)

```bash
cd python/nanots-py
maturin develop --release
```

## Project conventions

### Code style

- Prefer clear, explicit names and small, composable functions.
- Keep public APIs documented and add usage notes when behavior is subtle.
- Avoid unnecessary allocations and prefer streaming or zero-copy paths.

### Formatting and linting

- Use `cargo fmt` for formatting.
- Use `cargo clippy --all-targets --all-features` for linting before submitting.

### Tests

- Add tests for new features and bug fixes when practical.
- Favor minimal, deterministic inputs that are easy to read.
- For format changes, update `SPEC.md` and add regression tests where possible.

## Performance changes

If you touch storage layout, codecs, or hot paths:

- Include a short benchmark note in the PR description.
- Mention dataset shape, row counts, and comparison to baseline.

## Pull request checklist

- Issue/intent is documented (issue or PR description)
- Code builds and tests pass locally
- Docs updated when behavior changes
- No unintended format or API breakage

## Reporting security issues

Do not open public issues for security-sensitive reports. Contact the maintainers directly.

## Release/version changes

- Version is tracked in `Cargo.toml` and `python/nanots-py/pyproject.toml`.
- If your change requires a version bump, call it out in the PR.

## Commit guidance

- Keep commits focused and logically grouped.
- Prefer descriptive commit messages (what and why).

Thanks again for contributing.
