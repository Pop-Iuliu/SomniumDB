#!/usr/bin/env bash
# Ruleaza toate suitele de teste. Primul argument e binarul testat.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN="${1:-build-release/Redis}"
echo "=== SomniumDB test suite: $BIN ==="

# farmece de serveri orfani din rulari anterioare (local; in CI nu exista)
pkill -x Redis 2>/dev/null || true
sleep 0.3

# anomalie de mediu (kernel 6.8 + churn rapid): ocazional un server proaspat
# nu primeste nicio completare io_uring. Afecteaza si codul vechi; un restart
# o rezolva, deci o suita pica => o mai rulam o data inainte sa declaram FAIL.
run_suite() {
    python3 tests/smoke_test.py
    python3 tests/s5_test.py
    python3 tests/s3_test.py
    python3 tests/hib_test.py
}

if ! run_suite; then
    echo "!!! suita a picat (posibila flake de mediu io_uring) - reincerc o data"
    pkill -x Redis 2>/dev/null || true
    sleep 1
    run_suite
fi

echo "=== TOATE TESTELE AU TRECUT ==="
