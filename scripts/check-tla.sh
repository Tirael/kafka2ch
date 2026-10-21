#!/usr/bin/env bash
# Runs TLC on every specs/*.cfg.
# A cfg whose first line is `\* EXPECT-VIOLATION: <Invariant>` must fail with exactly
# that invariant violated; every other cfg must finish without errors.
set -euo pipefail

TLA_TOOLS_VERSION="1.7.4"
TLA_TOOLS_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_TOOLS_VERSION}/tla2tools.jar"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
specs_dir="$repo_root/specs"
work_dir="${TLC_WORK_DIR:-$repo_root/artifacts/tlc}"
jar="${TLA2TOOLS_JAR:-$work_dir/tla2tools-${TLA_TOOLS_VERSION}.jar}"

mkdir -p "$work_dir"

if [[ ! -f "$jar" ]]; then
  echo "Downloading tla2tools.jar v${TLA_TOOLS_VERSION}"
  curl -fsSL --retry 4 --retry-delay 4 -o "$jar.tmp" "$TLA_TOOLS_URL"
  mv "$jar.tmp" "$jar"
fi

echo "${TLA_TOOLS_SHA256}  ${jar}" | sha256sum -c -

shopt -s nullglob
cfgs=("$specs_dir"/*.cfg)
if [[ ${#cfgs[@]} -eq 0 ]]; then
  echo "No .cfg files in $specs_dir" >&2
  exit 1
fi

failures=0
summary=()

for cfg in "${cfgs[@]}"; do
  name="$(basename "$cfg" .cfg)"
  module="${name%%.*}"
  spec="$specs_dir/$module.tla"
  expected="$(sed -n '1s/^\\\* EXPECT-VIOLATION: *\([A-Za-z0-9_]*\).*$/\1/p' "$cfg" | tr -d '\r')"
  log="$work_dir/$name.log"

  if [[ ! -f "$spec" ]]; then
    echo "::error file=specs/$name.cfg::No module $module.tla for $name.cfg"
    failures=$((failures + 1))
    continue
  fi

  echo "::group::TLC $name"
  start=$SECONDS
  set +e
  (cd "$specs_dir" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC \
      -workers auto -cleanup -metadir "$work_dir/states/$name" \
      -config "$cfg" "$module.tla") >"$log" 2>&1
  status=$?
  set -e
  elapsed=$((SECONDS - start))
  cat "$log"
  echo "::endgroup::"

  states="$(grep -Eo '[0-9,]+ distinct states found' "$log" | tail -1 | cut -d' ' -f1 || true)"
  violated="$(grep -Eo 'Invariant [A-Za-z0-9_]+ is violated' "$log" | head -1 | cut -d' ' -f2 || true)"

  if [[ -z "$expected" ]]; then
    if [[ $status -eq 0 ]] && grep -q 'No error has been found' "$log"; then
      result="pass"
    else
      result="FAIL (exit $status${violated:+, $violated violated})"
    fi
  else
    if [[ "$violated" == "$expected" ]]; then
      result="expected violation of $expected"
    else
      result="FAIL (expected $expected violation, exit $status${violated:+, got $violated})"
    fi
  fi

  if [[ "$result" == FAIL* ]]; then
    echo "::error file=specs/$name.cfg::$result"
    failures=$((failures + 1))
  fi

  summary+=("$(printf '%-68s %-12s %5ss  %s' "$name" "${states:--}" "$elapsed" "$result")")
done

echo
printf '%-68s %-12s %6s  %s\n' "config" "states" "time" "result"
printf '%s\n' "${summary[@]}"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "| config | distinct states | time | result |"
    echo "| --- | ---: | ---: | --- |"
    for line in "${summary[@]}"; do
      read -r c s t r <<<"$line"
      echo "| \`$c\` | $s | $t | $r |"
    done
  } >>"$GITHUB_STEP_SUMMARY"
fi

if [[ $failures -gt 0 ]]; then
  echo "$failures TLC check(s) failed" >&2
  exit 1
fi
