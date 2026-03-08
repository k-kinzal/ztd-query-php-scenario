#!/usr/bin/env bash
set -euo pipefail

# PHP version matrix runner using Docker.
#
# Tests the suite against each supported PHP version (8.1–8.4)
# using Docker containers. SQLite tests run by default; use profiles
# for MySQL/PostgreSQL.
#
# Usage:
#   ./scripts/run-php-version-matrix.sh                    # SQLite only
#   ./scripts/run-php-version-matrix.sh --profile mysql    # + MySQL tests
#   ./scripts/run-php-version-matrix.sh --profile postgres # + PostgreSQL tests
#   ./scripts/run-php-version-matrix.sh --profile all      # all databases
#   ./scripts/run-php-version-matrix.sh --php 8.1          # single PHP version

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MATRIX_DIR="$PROJECT_DIR/build/matrix"

PHP_VERSIONS=("8.1" "8.2" "8.3" "8.4")
PROFILE=""
SINGLE_PHP=""

for arg in "$@"; do
    case "$arg" in
        --profile)  shift; PROFILE="$1"; shift ;;
        --php)      shift; SINGLE_PHP="$1"; shift ;;
        *)          ;;
    esac
done

if [ -n "$SINGLE_PHP" ]; then
    PHP_VERSIONS=("$SINGLE_PHP")
fi

mkdir -p "$MATRIX_DIR"

echo "=== PHP Version Matrix ==="
echo "PHP versions: ${PHP_VERSIONS[*]}"
echo "Profile: ${PROFILE:-sqlite-only}"
echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

PASS_COUNT=0
FAIL_COUNT=0

for php_ver in "${PHP_VERSIONS[@]}"; do
    short_ver=$(echo "$php_ver" | tr '.' '-')
    service="php${short_ver}-sqlite"
    log_file="$MATRIX_DIR/php${short_ver}-sqlite.log"

    echo "--- PHP $php_ver (SQLite) ---"

    exit_code=0
    docker compose -f "$PROJECT_DIR/docker-compose.yml" \
        run --rm --build "$service" \
        --log-junit "/app/build/matrix/php${short_ver}-sqlite.junit.xml" \
        > "$log_file" 2>&1 || exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "  PASS"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        # Extract summary line
        summary=$(grep -E 'Tests:|OK ' "$log_file" | tail -1)
        echo "  EXIT $exit_code: $summary"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    echo ""

    # Run database-specific tests if profile is set
    if [ -n "$PROFILE" ]; then
        if [ "$PROFILE" = "mysql" ] || [ "$PROFILE" = "all" ]; then
            mysql_service="php${short_ver}-mysql"
            mysql_log="$MATRIX_DIR/php${short_ver}-mysql.log"

            echo "--- PHP $php_ver (MySQL) ---"
            exit_code=0
            docker compose -f "$PROJECT_DIR/docker-compose.yml" \
                --profile mysql \
                run --rm --build "$mysql_service" \
                --log-junit "/app/build/matrix/php${short_ver}-mysql.junit.xml" \
                > "$mysql_log" 2>&1 || exit_code=$?

            if [ $exit_code -eq 0 ]; then
                echo "  PASS"
                PASS_COUNT=$((PASS_COUNT + 1))
            else
                summary=$(grep -E 'Tests:|OK ' "$mysql_log" | tail -1)
                echo "  EXIT $exit_code: $summary"
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
            echo ""
        fi

        if [ "$PROFILE" = "postgres" ] || [ "$PROFILE" = "all" ]; then
            pg_service="php${short_ver}-postgres"
            pg_log="$MATRIX_DIR/php${short_ver}-postgres.log"

            echo "--- PHP $php_ver (PostgreSQL) ---"
            exit_code=0
            docker compose -f "$PROJECT_DIR/docker-compose.yml" \
                --profile postgres \
                run --rm --build "$pg_service" \
                --log-junit "/app/build/matrix/php${short_ver}-postgres.junit.xml" \
                > "$pg_log" 2>&1 || exit_code=$?

            if [ $exit_code -eq 0 ]; then
                echo "  PASS"
                PASS_COUNT=$((PASS_COUNT + 1))
            else
                summary=$(grep -E 'Tests:|OK ' "$pg_log" | tail -1)
                echo "  EXIT $exit_code: $summary"
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
            echo ""
        fi
    fi
done

echo "=== Summary ==="
echo "Pass: $PASS_COUNT | Fail: $FAIL_COUNT"
echo "Logs: $MATRIX_DIR/"
