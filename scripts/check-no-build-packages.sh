#!/bin/bash
# Verify `[tool.uv].no-build-package` in pyproject.toml lists exactly the
# third-party packages locked in the corresponding uv.lock.
#
# uv has no native "block third-party sdist builds but not the workspace project"
# setting, so we maintain `no-build-package` as an explicit list of every locked
# third-party package. This script catches drift between the lock and the list,
# and is wired into pre-commit on changes to uv.lock or pyproject.toml.
#
#     scripts/check-no-build-packages.sh
#
# Exits 0 when the lock and manifest are in sync, 1 otherwise.

set -euo pipefail

for tool in awk sort comm uniq diff mktemp; do
  command -v "$tool" > /dev/null || {
    echo "Required tool not on PATH: $tool" >&2
    exit 2
  }
done

check_tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/check-no-build-packages.XXXXXX")
trap 'rm -rf "$check_tmp_dir"' EXIT

# Emit the names of third-party packages in a uv.lock - i.e. every package
# whose source is a registry/git/url. Workspace members (source.editable or
# source.virtual, or no source at all) are skipped.
locked_third_party() {
  awk '
    function flush() {
      if (in_pkg && have_source && !is_local && name != "") print name
    }
    /^\[\[package\]\]/ {
      flush()
      in_pkg=1; name=""; have_source=0; is_local=0
      next
    }
    /^\[/ {
      flush()
      in_pkg=0; name=""; have_source=0; is_local=0
      next
    }
    in_pkg && /^name = "/ {
      n=$0
      sub(/^name = "/, "", n)
      sub(/"$/, "", n)
      name=n
    }
    in_pkg && /^source = / {
      have_source=1
      if ($0 ~ /editable|virtual/) is_local=1
    }
    END { flush() }
  ' "$1" | LC_ALL=C sort -u
}

# Emit the entries of `tool.uv.no-build-package` from a pyproject.toml in
# source order (no sort), one per line. The block is expected to use the
# multi-line array form that taplo emits.
declared_packages() {
  awk '
    /^no-build-package = \[/ { in_list=1; next }
    in_list && /^\]/ { in_list=0; next }
    in_list && /^[[:space:]]*"/ {
      line=$0
      sub(/^[[:space:]]*"/, "", line)
      sub(/",?[[:space:]]*$/, "", line)
      print line
    }
  ' "$1"
}

pairs=(
  "python/uv.lock:python/pyproject.toml"
)

failures=0

for pair in "${pairs[@]}"; do
  lock="${pair%:*}"
  manifest="${pair#*:}"

  if [[ ! -f "$lock" || ! -f "$manifest" ]]; then
    echo "ERROR: missing $lock or $manifest" >&2
    exit 2
  fi

  locked_third_party "$lock" > "$check_tmp_dir/locked"
  declared_packages "$manifest" > "$check_tmp_dir/declared"
  LC_ALL=C sort "$check_tmp_dir/declared" > "$check_tmp_dir/sorted"
  uniq "$check_tmp_dir/sorted" > "$check_tmp_dir/unique"

  missing=$(comm -23 "$check_tmp_dir/locked" "$check_tmp_dir/unique")
  stale=$(comm -13 "$check_tmp_dir/locked" "$check_tmp_dir/unique")
  duplicates=$(uniq -d "$check_tmp_dir/sorted")
  out_of_order=""
  if ! diff -q "$check_tmp_dir/declared" "$check_tmp_dir/sorted" > /dev/null; then
    out_of_order="yes"
  fi

  if [[ -z "$missing" && -z "$stale" && -z "$duplicates" && -z "$out_of_order" ]]; then
    count=$(grep -c . "$check_tmp_dir/locked")
    echo "OK  ${manifest}: ${count} packages, in sync with ${lock}"
    continue
  fi

  failures=$((failures + 1))
  echo "FAIL ${manifest}: out of sync with ${lock}"

  if [[ -n "$missing" ]]; then
    count=$(printf '%s\n' "$missing" | grep -c .)
    echo "  Missing from no-build-package (${count}):"
    printf '%s\n' "$missing" | sed 's/^/    + /'
  fi
  if [[ -n "$stale" ]]; then
    count=$(printf '%s\n' "$stale" | grep -c .)
    echo "  Listed in no-build-package but not in lock (${count}):"
    printf '%s\n' "$stale" | sed 's/^/    - /'
  fi
  if [[ -n "$duplicates" ]]; then
    dup_line=$(printf '%s' "$duplicates" | tr '\n' ' ')
    echo "  Duplicate entries:${dup_line}"
  fi
  if [[ -n "$out_of_order" ]]; then
    echo "  Entries are not sorted alphabetically."
  fi
done

if ((failures > 0)); then
  echo >&2
  echo "Fix by updating no-build-package in the failing manifest(s) to match uv.lock." >&2
  exit 1
fi
