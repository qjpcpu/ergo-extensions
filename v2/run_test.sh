#!/usr/bin/env bash

set -euo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$module_dir"

if rg -n '[一-龥]' --glob '*.go' --glob '*.md' .; then
    echo "v2 Go and Markdown files must use English only" >&2
    exit 1
fi

if rg -n 'github\.com/qjpcpu/ergo-extensions/(app|system|registrar)(/|\")' .; then
    echo "v2 must not import packages from the v1 module" >&2
    exit 1
fi

coverage_file="$(mktemp)"
trap 'rm -f "$coverage_file"' EXIT

go test ./... -coverprofile="$coverage_file"
coverage="$(go tool cover -func="$coverage_file" | awk '/^total:/ {gsub("%", "", $3); print $3}')"
awk -v coverage="$coverage" 'BEGIN { if (coverage + 0 < 85) exit 1 }' || {
    echo "unit test coverage is ${coverage}%; at least 85% is required" >&2
    exit 1
}

go vet ./...

# Ergo v1.999.320 has races in its real-node startup and shutdown paths. Run
# the race detector across packages and focused routing tests that do not
# trigger those upstream paths; real-node integration remains covered above.
go test -race \
    ./registrar/mem \
    ./system/cron \
    ./system/daemon \
    ./system/internal/core \
    ./system/membership
go test -race ./system -run 'Test(ActorRouter|ActorRoute|Route|Routed|Renewal|TimingWheel|ReleaseQueue|PersistencePanic|SlowPersistence|ExitStorm)'

echo "v2 checks passed with ${coverage}% unit test coverage"
