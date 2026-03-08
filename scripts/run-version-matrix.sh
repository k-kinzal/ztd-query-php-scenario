#!/usr/bin/env bash
set -euo pipefail

# Cross-version test matrix runner.
#
# Runs the test suite against each supported database version combination
# and captures a baseline for each. Results are stored in build/matrix/.
#
# Usage:
#   ./scripts/run-version-matrix.sh              # run all combinations
#   ./scripts/run-version-matrix.sh --quick       # run boundary versions only
#   ./scripts/run-version-matrix.sh --mysql-only   # run MySQL variants only
#   ./scripts/run-version-matrix.sh --postgres-only # run PostgreSQL variants only
#   ./scripts/run-version-matrix.sh --sqlite-only   # run SQLite tests only
#
# Requirements:
#   - Docker
#   - PHP CLI with required extensions
#   - Composer dependencies installed

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MATRIX_DIR="$PROJECT_DIR/build/matrix"
SUMMARY_FILE="$MATRIX_DIR/summary.json"

# Supported version ranges
MYSQL_VERSIONS_ALL=("mysql:5.6" "mysql:5.7" "mysql:8.0" "mysql:8.4" "container-registry.oracle.com/mysql/community-server:9.1.0")
MYSQL_VERSIONS_QUICK=("mysql:5.7" "mysql:8.0" "container-registry.oracle.com/mysql/community-server:9.1.0")

POSTGRES_VERSIONS_ALL=("postgres:14" "postgres:15" "postgres:16" "postgres:17")
POSTGRES_VERSIONS_QUICK=("postgres:14" "postgres:16" "postgres:17")

# Parse flags
MODE="all"
RUN_MYSQL=true
RUN_POSTGRES=true
RUN_SQLITE=true

for arg in "$@"; do
    case "$arg" in
        --quick)       MODE="quick" ;;
        --mysql-only)  RUN_POSTGRES=false; RUN_SQLITE=false ;;
        --postgres-only) RUN_MYSQL=false; RUN_SQLITE=false ;;
        --sqlite-only) RUN_MYSQL=false; RUN_POSTGRES=false ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

if [ "$MODE" = "quick" ]; then
    MYSQL_VERSIONS=("${MYSQL_VERSIONS_QUICK[@]}")
    POSTGRES_VERSIONS=("${POSTGRES_VERSIONS_QUICK[@]}")
else
    MYSQL_VERSIONS=("${MYSQL_VERSIONS_ALL[@]}")
    POSTGRES_VERSIONS=("${POSTGRES_VERSIONS_ALL[@]}")
fi

mkdir -p "$MATRIX_DIR"

PHP_VERSION=$(php -r 'echo PHP_VERSION;')
echo "=== Cross-Version Test Matrix ==="
echo "PHP: $PHP_VERSION"
echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

RESULTS=()
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

run_suite() {
    local label="$1"
    local filter="$2"
    local env_vars="$3"
    local baseline_name="$4"

    echo "--- $label ---"
    local junit_file="$MATRIX_DIR/${baseline_name}.junit.xml"
    local baseline_file="$MATRIX_DIR/${baseline_name}.baseline.json"
    local log_file="$MATRIX_DIR/${baseline_name}.log"

    # Run tests
    local exit_code=0
    env $env_vars php "$PROJECT_DIR/vendor/bin/phpunit" \
        --testsuite Scenario \
        --filter "$filter" \
        --log-junit "$junit_file" \
        > "$log_file" 2>&1 || exit_code=$?

    # Capture baseline
    if [ -f "$junit_file" ] && [ -s "$junit_file" ]; then
        php "$SCRIPT_DIR/capture-baseline.php" \
            --junit "$junit_file" \
            --versions "$PROJECT_DIR/spec/verification-log.json" \
            -o "$baseline_file" 2>/dev/null || true

        local tests=$(grep -o 'tests="[0-9]*"' "$junit_file" | head -1 | grep -o '[0-9]*')
        local failures=$(grep -o 'failures="[0-9]*"' "$junit_file" | head -1 | grep -o '[0-9]*')
        local errors=$(grep -o 'errors="[0-9]*"' "$junit_file" | head -1 | grep -o '[0-9]*')
        local skipped=$(grep -o 'skipped="[0-9]*"' "$junit_file" | head -1 | grep -o '[0-9]*')
        tests=${tests:-0}
        failures=${failures:-0}
        errors=${errors:-0}
        skipped=${skipped:-0}

        local status="pass"
        if [ "$failures" -gt 0 ] || [ "$errors" -gt 0 ]; then
            status="fail"
            FAIL_COUNT=$((FAIL_COUNT + 1))
        else
            PASS_COUNT=$((PASS_COUNT + 1))
        fi

        echo "  Tests: $tests | Failures: $failures | Errors: $errors | Skipped: $skipped | Status: $status"
        RESULTS+=("{\"label\":\"$label\",\"tests\":$tests,\"failures\":$failures,\"errors\":$errors,\"skipped\":$skipped,\"status\":\"$status\",\"baseline\":\"$baseline_name\"}")
    else
        echo "  SKIP: No test output (image pull failed or tests crashed)"
        SKIP_COUNT=$((SKIP_COUNT + 1))
        RESULTS+=("{\"label\":\"$label\",\"tests\":0,\"failures\":0,\"errors\":0,\"skipped\":0,\"status\":\"skip\",\"baseline\":\"$baseline_name\"}")
    fi
    echo ""
}

# --- MySQL versions ---
if [ "$RUN_MYSQL" = true ]; then
    for mysql_img in "${MYSQL_VERSIONS[@]}"; do
        short_name=$(echo "$mysql_img" | sed 's|container-registry.oracle.com/mysql/community-server:||;s|mysql:||;s|\.|-|g')
        # MySQLi adapter
        run_suite \
            "MySQLi + MySQL $mysql_img" \
            "Tests\\\\Mysqli\\\\" \
            "MYSQL_IMAGE=$mysql_img" \
            "mysqli-mysql${short_name}"

        # MySQL PDO adapter
        run_suite \
            "MySQL-PDO + MySQL $mysql_img" \
            "Tests\\\\Pdo\\\\Mysql" \
            "MYSQL_IMAGE=$mysql_img" \
            "mysql-pdo-mysql${short_name}"
    done
fi

# --- PostgreSQL versions ---
if [ "$RUN_POSTGRES" = true ]; then
    for pg_img in "${POSTGRES_VERSIONS[@]}"; do
        short_name=$(echo "$pg_img" | sed 's|postgres:||;s|\.|-|g')
        run_suite \
            "PostgreSQL-PDO + PostgreSQL $pg_img" \
            "Tests\\\\Pdo\\\\Postgres" \
            "POSTGRES_IMAGE=$pg_img" \
            "postgres-pdo-pg${short_name}"
    done
fi

# --- SQLite ---
if [ "$RUN_SQLITE" = true ]; then
    run_suite \
        "SQLite-PDO + SQLite (bundled)" \
        "Tests\\\\Pdo\\\\Sqlite" \
        "" \
        "sqlite-pdo"
fi

# --- Summary ---
echo "=== Matrix Summary ==="
echo "Pass: $PASS_COUNT | Fail: $FAIL_COUNT | Skip: $SKIP_COUNT"
echo ""

# Write summary JSON
RESULTS_JSON=$(printf '%s\n' "${RESULTS[@]}" | paste -sd',' -)
cat > "$SUMMARY_FILE" <<EOF
{
  "phpVersion": "$PHP_VERSION",
  "capturedAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "pass": $PASS_COUNT,
  "fail": $FAIL_COUNT,
  "skip": $SKIP_COUNT,
  "results": [$RESULTS_JSON]
}
EOF

echo "Summary written to: $SUMMARY_FILE"
echo "Individual baselines in: $MATRIX_DIR/"
