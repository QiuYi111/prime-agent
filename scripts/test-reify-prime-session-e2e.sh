#!/usr/bin/env bash
set -euo pipefail

readonly expected_reify_revision="e0b453f90fe1a839f8cf1090ac18f46bf6057668"
reify_root="${1:-${PI_CAD_TEST_REIFY_ROOT:-}}"

if [[ -z "$reify_root" ]]; then
	echo "Usage: $0 /path/to/pi-cad (must be clean at $expected_reify_revision)" >&2
	exit 2
fi
if [[ ! -d "$reify_root" ]]; then
	echo "Reify root is not a directory: $reify_root" >&2
	exit 2
fi
reify_root="$(cd -- "$reify_root" && pwd -P)"

if [[ "$(uname -s)" != Linux ]]; then
	echo "This integration test requires Linux (bubblewrap and Unix sockets)." >&2
	exit 2
fi
if [[ ! -f "$reify_root/package.json" || ! -f "$reify_root/src/authority/sidecar.ts" ]]; then
	echo "Not a Reify checkout: missing package.json or src/authority/sidecar.ts" >&2
	exit 2
fi
actual_revision="$(git -C "$reify_root" rev-parse HEAD 2>/dev/null)" || {
	echo "Reify root is not a Git checkout: $reify_root" >&2
	exit 2
}
if [[ "$actual_revision" != "$expected_reify_revision" ]]; then
	echo "Wrong Reify revision: expected $expected_reify_revision, got $actual_revision" >&2
	exit 2
fi
if [[ -n "$(git -C "$reify_root" status --porcelain --untracked-files=all)" ]]; then
	echo "Reify checkout must be clean at $expected_reify_revision" >&2
	exit 2
fi

prime_root="$(cd -- "$(dirname -- "$0")/.." && pwd -P)"
if [[ ! -f "$prime_root/packages/coding-agent/package.json" || ! -f "$prime_root/packages/coding-agent/test/reify-prime-authority-e2e.test.ts" ]]; then
	echo "Run this script from a complete Prime Agent checkout." >&2
	exit 2
fi

echo "Prime: $prime_root"
echo "Reify: $reify_root ($actual_revision)"
cd "$prime_root/packages/coding-agent"
PI_CAD_TEST_REIFY_ROOT="$reify_root" npm test -- test/reify-prime-authority-e2e.test.ts
