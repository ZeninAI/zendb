#!/usr/bin/env bash
set -euo pipefail

version="${1:?release version is required}"
root="$(git rev-parse --show-toplevel)"
cd "$root"
crates_io_user_agent="zendb-release-workflow (https://github.com/zenin/zendb)"

crate_url() {
  printf 'https://crates.io/api/v1/crates/%s/%s' "$1" "$2"
}

wait_for_crate() {
  local crate="$1"
  local crate_version="$2"
  local attempt

  for attempt in $(seq 1 60); do
    if curl --fail --silent --user-agent "$crates_io_user_agent" \
      "$(crate_url "$crate" "$crate_version")" >/dev/null; then
      return 0
    fi
    sleep 5
  done

  echo "Timed out waiting for $crate $crate_version to become available on crates.io." >&2
  return 1
}

publish_crate() {
  local crate="$1"

  if curl --fail --silent --user-agent "$crates_io_user_agent" \
    "$(crate_url "$crate" "$version")" >/dev/null; then
    echo "$crate $version is already published; continuing." >&2
    return 0
  fi

  echo "Publishing $crate $version." >&2
  cargo publish --package "$crate" --locked --allow-dirty >&2
  wait_for_crate "$crate" "$version"
}

# Workspace dependencies must be visible on crates.io before dependants publish.
publish_crate zendb-types
publish_crate zendb-storage
publish_crate zendb-workspace
