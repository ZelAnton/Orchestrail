#!/usr/bin/env bash
#
# Runs mutation testing for Orchestrail's deterministic algorithmic layers.
# The clean engine test suite is run twice first because cargo-mutants has no
# min_test_passes configuration setting. Additional arguments are passed to
# cargo-mutants, for example: ./scripts/run-mutants.sh --file 'engine/src/resolvers/**/*.rs'
#
# Exit 0 means mutation analysis completed; surviving mutants remain
# informational. Setup, configuration, baseline, and internal errors fail.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
output_dir="$repo_root/mutants.out"

count_nonempty_lines() {
  local path="$1"
  if [[ -f "$path" ]]; then
    awk 'NF { count++ } END { print count + 0 }' "$path"
  else
    printf '0\n'
  fi
}

cd "$repo_root"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not on PATH" >&2
  exit 1
fi
if ! cargo mutants --version >/dev/null 2>&1; then
  echo "error: cargo-mutants is not installed; run 'cargo install --locked cargo-mutants'" >&2
  exit 1
fi

echo "==> Verifying the clean engine tests (pass 1 of 2)"
cargo test --package orchestrail-engine
echo "==> Verifying the clean engine tests (pass 2 of 2)"
cargo test --package orchestrail-engine

echo "==> Running cargo-mutants against configured pure layers"
set +e
cargo mutants --config .cargo-mutants.toml "$@"
mutants_status=$?
set -e

if [[ ! -f "$output_dir/outcomes.json" || ! -f "$output_dir/mutants.json" ]]; then
  echo "error: cargo-mutants did not produce complete results in $output_dir" >&2
  exit "${mutants_status:-1}"
fi

total=$(awk '/"name"[[:space:]]*:/ { count++ } END { print count + 0 }' "$output_dir/mutants.json")
caught=$(count_nonempty_lines "$output_dir/caught.txt")
survived=$(count_nonempty_lines "$output_dir/missed.txt")
unviable=$(count_nonempty_lines "$output_dir/unviable.txt")
timeouts=$(count_nonempty_lines "$output_dir/timeout.txt")
viable=$((caught + survived))
if (( viable > 0 )); then
  survival_rate=$(awk -v survived="$survived" -v viable="$viable" 'BEGIN { printf "%.2f", 100 * survived / viable }')
else
  survival_rate="0.00"
fi

summary=$(cat <<EOF
Mutation testing summary
  Total mutants generated: $total
  Caught by tests:         $caught
  Surviving mutants:       $survived
  Survival rate:           $survival_rate% (surviving / viable)
  Unviable mutants:        $unviable
  Timed-out mutants:       $timeouts

Detailed results: $output_dir
Inspect missed.txt for survivors, outcomes.json for machine-readable results,
and diff/ plus log/ for each mutation's source change and test output.
EOF
)
printf '%s\n' "$summary"
printf '%s\n' "$summary" >"$output_dir/summary.txt"

# cargo-mutants uses 2 for survivors and 3 for timeouts. Both mean the analysis
# completed and are informational for this project; all other failures propagate.
case "$mutants_status" in
  0|2|3) exit 0 ;;
  *) exit "$mutants_status" ;;
esac
