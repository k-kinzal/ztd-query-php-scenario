#!/usr/bin/env bash
set -euo pipefail

# Test PHP 8.1–8.5 (upstream supports PHP 8.1+).
# SQLite runs by default. Add --profile mysql|postgres|all for databases.
# Use --php 8.1 to select one PHP version.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MATRIX_DIR="$PROJECT_DIR/build/matrix"
PHP_VERSIONS=("8.1" "8.2" "8.3" "8.4" "8.5")
PROFILE=""
SINGLE_PHP=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --profile|--php)
            if [ "$#" -lt 2 ]; then
                echo "Missing value for $1" >&2
                exit 1
            fi
            case "$1" in
                --profile) PROFILE="$2" ;;
                --php) SINGLE_PHP="$2" ;;
            esac
            shift 2
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

case "$PROFILE" in
    ""|mysql|postgres|all) ;;
    *) echo "Unknown profile: $PROFILE" >&2; exit 1 ;;
esac
if [ -n "$SINGLE_PHP" ]; then
    case "$SINGLE_PHP" in
        8.1|8.2|8.3|8.4|8.5) PHP_VERSIONS=("$SINGLE_PHP") ;;
        *) echo "PHP version is not configured: $SINGLE_PHP" >&2; exit 1 ;;
    esac
fi

mkdir -p "$MATRIX_DIR"
echo "=== PHP Version Matrix ==="
echo "PHP versions: ${PHP_VERSIONS[*]}"
echo "Profile: ${PROFILE:-sqlite-only}"

PASS_COUNT=0
FAIL_COUNT=0

run_suite() {
    local php_ver="$1"
    local database="$2"
    local filter="$3"
    local service="php${php_ver//./}-${database}"
    local result_name="php${php_ver//./-}-${database}"
    local log_file="$MATRIX_DIR/${result_name}.log"
    local compose_args=(-f "$PROJECT_DIR/docker-compose.yml")
    if [ "$database" != sqlite ]; then
        compose_args+=(--profile "$database")
    fi

    echo "--- PHP $php_ver ($database) ---"
    local exit_code=0
    # Explicit filters are needed because arguments to `compose run` replace CMD.
    docker compose "${compose_args[@]}" run --rm --build "$service" \
        --testsuite Scenario --filter "$filter" \
        --log-junit "/app/build/matrix/${result_name}.junit.xml" \
        > "$log_file" 2>&1 || exit_code=$?

    if [ "$exit_code" -eq 0 ]; then
        echo "  PASS"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        local summary
        summary=$(grep -E 'Tests:|OK ' "$log_file" | tail -1 || true)
        echo "  EXIT $exit_code: ${summary:-See $log_file}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
}

for php_ver in "${PHP_VERSIONS[@]}"; do
    run_suite "$php_ver" sqlite 'Tests\\Pdo\\Sqlite'
    if [ "$PROFILE" = mysql ] || [ "$PROFILE" = all ]; then
        run_suite "$php_ver" mysql 'Tests\\Pdo\\Mysql|Tests\\Mysqli\\'
    fi
    if [ "$PROFILE" = postgres ] || [ "$PROFILE" = all ]; then
        run_suite "$php_ver" postgres 'Tests\\Pdo\\Postgres'
    fi
done

echo "=== Summary ==="
echo "Pass: $PASS_COUNT | Fail: $FAIL_COUNT"
echo "Logs: $MATRIX_DIR/"
[ "$FAIL_COUNT" -eq 0 ]
