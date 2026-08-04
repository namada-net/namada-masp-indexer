help:
    @echo Available commands: `just --summary`

clean:
    cargo clean

fmt *CHECK:
    cargo fmt --all {{ if CHECK == "check" { "-- --check" } else { "" } }}

clippy:
    cargo clippy

clippy-fix:
    cargo clippy --fix --allow-dirty --allow-staged

check:
    cargo check

build:
    cargo build 

build-release:
    cargo build --release
