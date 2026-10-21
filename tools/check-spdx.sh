#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Checks SPDX headers and the core/ee boundary (ADR-0022).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

fail=0
files=$(git ls-files --cached --others --exclude-standard \
  '*.rs' '*.kt' '*.kts' '*.ex' '*.exs' '*.ts' '*.proto' '*.sh' '*.py' '*.sql' '*.cql' \
  'Cargo.toml' 'buf.yaml' 'docker-compose.yml' 'Makefile' '.github/workflows/*.yml' | grep -v '^docs/' || true)

for f in $files; do
  [[ -f "$f" ]] || continue
  header=$(head -n 3 "$f")
  if ! grep -q 'SPDX-License-Identifier:' <<<"$header"; then
    echo "missing SPDX header: $f"; fail=1; continue
  fi
  case "$f" in
    */ee/*|ee/*) want='LicenseRef-xmsg-EE' ;;
    proto/*|spec/*|clients/*) want='Apache-2.0' ;;
    *) want='AGPL-3.0-only' ;;
  esac
  if ! grep -q "SPDX-License-Identifier: $want" <<<"$header"; then
    echo "wrong licence in $f (expected $want)"; fail=1
  fi
done

# Core code must never reference enterprise code.
if git grep -n -I -E '(^|[^a-z])ee/|dev\.xmsg\.ee\.|XmsgGateway\.EE\.|xmsg_ee::' -- ':!*/ee/*' ':!ee/*' ':!docs/*' ':!tools/check-spdx.sh' ':!CONTRIBUTING.md' ':!README.md' ':!.gitignore' ':!.github/*' >/dev/null 2>&1; then
  echo "core code references enterprise code:"
  git grep -n -I -E '(^|[^a-z])ee/|dev\.xmsg\.ee\.|XmsgGateway\.EE\.|xmsg_ee::' -- ':!*/ee/*' ':!ee/*' ':!docs/*' ':!tools/check-spdx.sh' ':!CONTRIBUTING.md' ':!README.md' ':!.gitignore' ':!.github/*'
  fail=1
fi

[[ $fail -eq 0 ]] && echo "SPDX headers and core/ee boundary OK"
exit $fail
