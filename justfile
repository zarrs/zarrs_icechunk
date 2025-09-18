TOOLCHAIN := "nightly"
export RUST_BACKTRACE := "0"

# Display the available recipes
_help:
    @just --list --unsorted

# Build (cargo check)
build:
	cargo +{{TOOLCHAIN}} check --all-features

# Run tests
test:
	cargo +{{TOOLCHAIN}} test --all-features

# Generate documentation
doc:
	RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +{{TOOLCHAIN}} doc -Z unstable-options -Z rustdoc-scrape-examples --all-features # --no-deps

# Run clippy linter
clippy:
	cargo +{{TOOLCHAIN}} clippy --all-features -- -D warnings

# Run rustfmt
fmt:
	cargo +{{TOOLCHAIN}} fmt

# Run all checks: fmt, build, test, clippy, doc
check: build test clippy doc
	cargo +{{TOOLCHAIN}} fmt --all -- --check
	cargo +{{TOOLCHAIN}} check
	cargo +{{TOOLCHAIN}} check --no-default-features

# Run the example that creates a Zarr store from a NetCDF file
example_virtualizarr_netcdf:
	rm -rf examples/data/test.icechunk.zarr
	./examples/virtualizarr_netcdf.py
	cargo run --example virtualizarr_netcdf
