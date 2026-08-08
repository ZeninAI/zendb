#!/usr/bin/env bash
set -euo pipefail

version="${1:?release version is required}"
root="$(git rev-parse --show-toplevel)"
cd "$root"

# cargo-edit updates every workspace package and its internal version requirements.
cargo install cargo-edit --version 0.13.13 --locked
cargo set-version --workspace "$version"

# Refresh only the workspace package entries in Cargo.lock, then verify that the
# lockfile is sufficient for the publish commands.
cargo check --workspace
cargo check --workspace --locked
