#!/usr/bin/env bash
# Verify every dependency's license against an allowlist. Cargo.lock pins
# the exact graph; a new dependency with an unknown license fails the check
# instead of silently entering the build.
set -euo pipefail
cd "$(dirname "$0")/.."

# SPDX identifiers allowed in the dependency graph.
ALLOWED='^(MIT|Apache-2\.0|Apache-2\.0 WITH LLVM-exception|LLVM-exception|BSD-3-Clause|ISC|Unicode-3\.0|Zlib|CDLA-Permissive-2\.0|MIT-0|CC0-1\.0|Unlicense|LGPL-2\.1-or-later|BSL-1\.0|MPL-2\.0)$'

cargo metadata --format-version 1 --locked > /tmp/alias-cache-metadata.json

violations=$(python3 - "$ALLOWED" << 'EOF'
import json, re, sys
allowed = re.compile(sys.argv[1])
metadata = json.load(open("/tmp/alias-cache-metadata.json"))
our_packages = {pkg["name"] for pkg in metadata["packages"] if pkg.get("source") is None}
bad = []
for pkg in metadata["packages"]:
    if pkg["name"] in our_packages:
        continue
    license_field = pkg.get("license") or ""
    if not license_field:
        bad.append(f'{pkg["name"]} {pkg["version"]}: NO LICENSE FIELD')
        continue
    # Normalize legacy "MIT/Apache-2.0" disjunctions to space-separated form.
    normalized = license_field.replace("/", " ")
    tokens = [
        token
        for token in normalized.replace("(", " ").replace(")", " ").split()
        if token not in ("AND", "OR", "WITH")
    ]
    if not all(allowed.match(token) for token in tokens):
        bad.append(f'{pkg["name"]} {pkg["version"]}: {license_field}')
print("\n".join(bad))
EOF
)

if [ -n "$violations" ]; then
    echo "Dependency license check failed:"
    echo "$violations"
    exit 1
fi
echo "All dependency licenses are on the allowlist."
