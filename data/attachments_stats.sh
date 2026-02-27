#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/home/Work/10-edu/data-science/thesis/code/masters-thesis-dev/data/raw/attachments"

if [ ! -d "$ROOT" ]; then
  echo "Attachments directory not found: $ROOT" >&2
  exit 1
fi

total_files=$(find "$ROOT" -type f -print | wc -l | tr -d ' ')
unique_files=$(find "$ROOT" -type f -print -exec shasum -a 256 {} + | awk '{print $1}' | sort -u | wc -l | tr -d ' ')

echo "Total files: $total_files"
echo "Unique files (sha256): $unique_files"
echo "Extension distribution:"

find "$ROOT" -type f -print | awk '{
  file=$0
  sub(/^.*\//, "", file)
  ext="(no_ext)"
  if (match(file, /\.([^.]+)$/)) {
    ext=tolower(substr(file, RSTART+1, RLENGTH-1))
  }
  print ext
}' | sort | uniq -c | sort -nr
