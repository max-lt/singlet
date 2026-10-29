[private]
default:
    @just --list

# fmt and clippy, for the workspace and the fuzz crate
check:
    cargo fmt --all --check
    cargo fmt --all --check --manifest-path fuzz/Cargo.toml
    cargo clippy --all-targets -- -D warnings
    cargo +nightly clippy --manifest-path fuzz/Cargo.toml -- -D warnings

test:
    cargo test

# Fuzz the runtime target for `secs` seconds
fuzz secs="300":
    cargo +nightly fuzz run runtime -- -max_len=512 -len_control=0 -max_total_time={{secs}}

# Remove the build output; keep the fuzz corpus
clean:
    cargo clean
    cargo clean --manifest-path fuzz/Cargo.toml
