#!/usr/bin/env bash
# Query throughput of the client against Go's pgx and Rust's tokio-postgres: WORKERS connections each run
# QUERIES sequential prepared point lookups. Needs a server (tests/start-postgres.sh) and its PG* variables.
# Usage: MS=path/to/ms bench/run.sh <samples>
set -euo pipefail
samples=${1:?usage: bench/run.sh <samples>}
here=$(cd "$(dirname "$0")" && pwd)
ms=${MS:-ms}
mkdir -p "$here/.build"
# The benchmark project keeps its imports inside its root, so it gets a copy of the package.
rm -rf "$here/ms/pg"
cp -r "$here/../src" "$here/ms/pg"
rm -f "$here/ms/ms.lock"
(cd "$here/ms" && "$ms" build ms.toml -o "$here/.build/ms")
(cd "$here/go" && CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w -buildid=' -o "$here/.build/go" .)
(cd "$here/rust" && cargo build --release --quiet)
"$here/.build/ms" setup > /dev/null
names=("Manuscript" "Go pgx" "Rust tokio-postgres")
exes=("$here/.build/ms" "$here/.build/go" "$here/rust/target/release/postgres-bench")
times=$(mktemp)
expected=""
for ((round = 0; round < samples; round++)); do
  for ((step = 0; step < 3; step++)); do
    which=$(((round + step) % 3))
    start=$(date +%s.%N)
    out=$("${exes[$which]}")
    end=$(date +%s.%N)
    out=$(echo "$out" | tail -1)
    [ -n "$expected" ] || expected=$out
    [ "$out" = "$expected" ] || { echo "${names[$which]} printed $out, expected $expected" >&2; exit 1; }
    echo "$which $(echo "$end - $start" | bc -l)" >> "$times"
  done
done
workers=${WORKERS:-4}
queries=${QUERIES:-20000}
echo "$expected; $workers connections x $queries lookups, median of $samples"
printf '%-22s%10s%12s\n' program seconds queries/s
for which in 0 1 2; do
  median=$(awk -v w="$which" '$1 == w { print $2 }' "$times" | sort -g | awk '{ v[NR] = $1 } END { print (NR % 2) ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2 }')
  printf '%-22s%10.3f%12.0f\n' "${names[$which]}" "$median" "$(echo "$workers * $queries / $median" | bc -l)"
done
rm -f "$times"
