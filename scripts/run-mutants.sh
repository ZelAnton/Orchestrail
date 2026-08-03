#!/usr/bin/env bash
#
# Runs mutation testing for Orchestrail's deterministic algorithmic layers.
# The clean engine test suite is run twice first because cargo-mutants has no
# min_test_passes configuration setting. Pass --quick for a two-stage smoke run:
# first validate the production .cargo-mutants.toml file list and its explicit
# integration-boundary exclusions, then analyze the narrow tiering resolver
# subset. Other arguments are passed to cargo-mutants; --list/--json are dry-run
# output overrides and intentionally fail this wrapper because they do not
# produce analysis reports.
#
# Exit 0 means mutation analysis completed; surviving mutants remain
# informational. Setup, configuration, baseline, and internal errors fail.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
output_dir="$repo_root/mutants.out"
config_file=".cargo-mutants.toml"
run_label="configured pure layers"
quick_mode=false

if [[ "${1:-}" == "--quick" ]]; then
  shift
  config_file=".cargo-mutants-quick.toml"
  run_label="quick tiering resolver subset"
  quick_mode=true
fi

for arg in "$@"; do
  case "$arg" in
    --list|--list-files|--json)
      echo "error: $arg is a cargo-mutants dry-run output mode and cannot verify analysis reports" >&2
      exit 1
      ;;
  esac
done

count_nonempty_lines() {
  local path="$1"
  if [[ -f "$path" ]]; then
    awk 'NF { count++ } END { print count + 0 }' "$path"
  else
    printf '0\n'
  fi
}

run_clean_tests() {
  if [[ "$quick_mode" == true ]]; then
    cargo test --package orchestrail-engine --lib -- resolvers::tiering
  else
    cargo test --package orchestrail-engine
  fi
}

validate_production_config() {
  local config_path="$repo_root/.cargo-mutants.toml"
  local exclude_globs
  local listed_files
  local normalized_files
  local required_file="engine/src/resolvers/tiering.rs"
  local excluded_file
  local excluded_files=(
    "engine/src/vcs.rs"
    "engine/src/headless.rs"
    "engine/src/supervise.rs"
    "engine/src/run.rs"
    "engine/src/notification.rs"
    "engine/src/verification.rs"
    "engine/src/legacy_fingerprint.rs"
  )

  if [[ ! -f "$config_path" ]]; then
    echo "error: production config is missing: $config_path" >&2
    return 1
  fi

  # Extract the TOML array directly, so a boundary cannot be silently omitted
  # while an examine_globs pattern happens not to select it. This intentionally
  # accepts the simple string-array form used by this repository's TOML config.
  if ! exclude_globs=$(awk '
    /^[[:space:]]*exclude_globs[[:space:]]*=[[:space:]]*\[[[:space:]]*$/ {
      if (found++) exit 1
      in_array = 1
      next
    }
    in_array && /^[[:space:]]*\][[:space:]]*$/ {
      closed = 1
      exit
    }
    in_array {
      if ($0 ~ /^[[:space:]]*$/ || $0 ~ /^[[:space:]]*#/) next
      if ($0 !~ /^[[:space:]]*"[^"]+"[[:space:]]*,?[[:space:]]*(#.*)?$/) exit 1
      value = $0
      sub(/^[[:space:]]*"/, "", value)
      sub(/"[[:space:]]*,?[[:space:]]*(#.*)?$/, "", value)
      print value
    }
    END { if (!found || !closed) exit 1 }
  ' "$config_path"); then
    echo "error: could not parse exclude_globs from $config_path" >&2
    return 1
  fi
  printf '%s\n' "validated TOML exclude_globs: $(awk 'NF { count++ } END { print count + 0 }' <<<"$exclude_globs") entries"

  for excluded_file in "${excluded_files[@]}"; do
    if ! grep -Fxq "$excluded_file" <<<"$exclude_globs"; then
      echo "error: production config must explicitly exclude $excluded_file" >&2
      return 1
    fi
    echo "validated explicit exclusion: $excluded_file"
  done

  # cargo-mutants parses the complete TOML document and applies it without
  # launching mutation tests. The assertions below also prove the selected
  # targets retain a deterministic file and omit every external boundary.
  listed_files=$(cargo mutants --config .cargo-mutants.toml --list-files)
  normalized_files=${listed_files//\\//}
  printf '%s\n' "$listed_files"

  if ! grep -Fq "$required_file" <<<"$normalized_files"; then
    echo "error: production config did not examine $required_file" >&2
    return 1
  fi
  for excluded_file in "${excluded_files[@]}"; do
    if grep -Fq "$excluded_file" <<<"$normalized_files"; then
      echo "error: production config unexpectedly examined $excluded_file" >&2
      return 1
    fi
  done
  echo "validated cargo-mutants production configuration"
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

if [[ "$quick_mode" == true ]]; then
  echo "==> Validating production cargo-mutants configuration and boundary exclusions"
  validate_production_config
fi

echo "==> Verifying the clean engine tests (pass 1 of 2)"
run_clean_tests
echo "==> Verifying the clean engine tests (pass 2 of 2)"
run_clean_tests

echo "==> Running cargo-mutants against $run_label"
set +e
cargo mutants --config "$config_file" "$@"
mutants_status=$?
set -e

if [[ ! -f "$output_dir/outcomes.json" || ! -f "$output_dir/mutants.json" ]]; then
  echo "error: cargo-mutants did not produce complete results in $output_dir" >&2
  # Listing/JSON-only invocations are dry runs and must not masquerade as a
  # completed analysis, even if cargo-mutants itself returned success.
  exit 1
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
