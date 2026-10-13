#!/usr/bin/env bash
# Ruleaza toate suitele de teste. Primul argument e binarul testat.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN="${1:-build-release/Redis}"
echo "=== SomniumDB test suite: $BIN ==="

# parserul RESP, fara server (construit langa binarul testat)
"$(dirname "$BIN")/resp_test"
"$(dirname "$BIN")/hlc_test"

# farmece de serveri orfani din rulari anterioare (local; in CI nu exista)
pkill -x Redis 2>/dev/null || true
sleep 0.3

# anomalie de mediu (kernel 6.8 + churn rapid): ocazional un server proaspat
# nu primeste nicio completare io_uring. Afecteaza si codul vechi; un restart
# o rezolva, deci o suita picata mai e rulata o data inainte sa declaram FAIL.
# A doua cadere opreste scriptul (set -e), deci CI-ul vede orice suita picata.
for suite in smoke s5 s4 s1 s2 s6 s7 s8 s9 s10 s11 s12 s13 s14 s18 s3 hib sec; do
    python3 "tests/${suite}_test.py" || {
        echo "!!! $suite a picat (posibila flake de mediu io_uring) - reincerc o data"
        pkill -x Redis 2>/dev/null || true
        sleep 1
        python3 "tests/${suite}_test.py"
    }
done

echo "=== TOATE TESTELE AU TRECUT ==="
