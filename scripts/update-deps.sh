#!/usr/bin/env bash
# Update all Go module dependencies for the library and the _examples module.
#
# Usage:
#   ./scripts/update-deps.sh          # minor/patch updates only
#   ./scripts/update-deps.sh --major  # also apply major version updates
set -euo pipefail

# Run from repo root (script lives in ./scripts/).
cd "$(dirname "$0")/.."

# Packages to exclude from automatic updates (updated manually), e.g.
# "github.com/some/module". Empty for now.
EXCLUDE=()

MAJOR_FLAG="${1:-}"

update_module() {
    local module_dir="$1"
    echo ""
    echo "############################################################"
    echo "# Updating module in: ${module_dir}"
    echo "############################################################"
    pushd "$module_dir" >/dev/null

    # Record current versions of excluded packages.
    local SAVED=()
    # ${arr[@]+"${arr[@]}"} keeps empty arrays working under set -u in bash 3.2 (macOS).
    for pkg in ${EXCLUDE[@]+"${EXCLUDE[@]}"}; do
        local ver
        ver=$(grep "^[[:space:]]*${pkg} " go.mod | awk '{print $2}' || true)
        if [[ -n "$ver" ]]; then
            SAVED+=("${pkg}@${ver}")
        fi
    done

    echo "==> Updating all dependencies to latest minor/patch versions..."
    go get -u ./...

    # Restore excluded packages to their original versions.
    for entry in ${SAVED[@]+"${SAVED[@]}"}; do
        echo "==> Pinning $entry (excluded from update)"
        go get "$entry"
    done

    echo "==> Running go mod tidy..."
    go mod tidy

    if [[ "$MAJOR_FLAG" == "--major" ]]; then
        if ! command -v gomajor &>/dev/null; then
            echo "gomajor not found, installing..."
            go install github.com/icholy/gomajor@latest
        fi
        echo "==> Checking for major version updates..."
        gomajor list
        echo ""
        read -rp "Apply all major version updates in ${module_dir}? [y/N] " answer
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            gomajor get -a ./...
            echo "==> Running go mod tidy after major updates..."
            go mod tidy
        fi
    fi

    popd >/dev/null
}

# Library module.
update_module "."

# _examples module (if present). Has its own go.mod with a `replace` pointing
# at the parent — the local Centrifuge dep tracks the parent automatically.
if [[ -f "_examples/go.mod" ]]; then
    update_module "_examples"
fi

echo ""
echo "==> Done. Review changes with: git diff go.mod _examples/go.mod"
