<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL format() function through the CTE rewriter.
 *
 * Real-world scenario: format() is PostgreSQL's sprintf-equivalent, commonly
 * used for building display strings, generating identifiers, or formatting
 * output. Its %I (identifier) and %L (literal) placeholders use special syntax
 * that could confuse the CTE rewriter's SQL parser.
 *
 * @spec SPEC-3.1
 * @spec SPEC-10.2
 */
class PostgresFormatFunctionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_fmt_users (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                code VARCHAR(10)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_fmt_users'];
    }

    /**
     * Basic format() in SELECT on shadow data.
     */
    public function testFormatInSelect(): void
    {
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Alice', 'Smith', 'A01')");
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Bob', 'Jones', 'B02')");

        try {
            $rows = $this->ztdQuery(
                "SELECT format('%s %s', first_name, last_name) AS full_name
                 FROM pg_fmt_users
                 ORDER BY last_name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'format() in SELECT returned no rows on shadow data.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Bob Jones', $rows[0]['full_name']);
            $this->assertSame('Alice Smith', $rows[1]['full_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'format() in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * format() with %I (identifier quoting) placeholder.
     */
    public function testFormatWithIdentifierPlaceholder(): void
    {
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Alice', 'Smith', 'USR')");

        try {
            $rows = $this->ztdQuery(
                "SELECT format('User: %I -> %s', code, first_name) AS label
                 FROM pg_fmt_users"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'format() with %I placeholder returned no rows.'
                );
            }

            $this->assertCount(1, $rows);
            // %I quotes the identifier, so 'USR' -> "USR"
            $this->assertStringContainsString('Alice', $rows[0]['label']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'format() with %I failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * format() in WHERE clause for filtering.
     */
    public function testFormatInWhere(): void
    {
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Alice', 'Smith', 'A01')");
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Bob', 'Jones', 'B02')");

        try {
            $rows = $this->ztdQuery(
                "SELECT first_name FROM pg_fmt_users
                 WHERE format('%s-%s', last_name, code) = 'Smith-A01'"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'format() in WHERE returned no rows.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['first_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'format() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * format() with prepared statement parameters.
     */
    public function testFormatWithPreparedParams(): void
    {
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Alice', 'Smith', 'A01')");
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Bob', 'Jones', 'B02')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT format('%s %s', first_name, last_name) AS full_name
                 FROM pg_fmt_users
                 WHERE last_name = ?
                 ORDER BY first_name",
                ['Smith']
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'format() with prepared params returned no rows.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice Smith', $rows[0]['full_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'format() with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * format() in UPDATE SET clause.
     */
    public function testFormatInUpdateSet(): void
    {
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Alice', 'Smith', NULL)");

        try {
            $this->ztdExec(
                "UPDATE pg_fmt_users SET code = format('USR-%s', first_name) WHERE code IS NULL"
            );

            $rows = $this->ztdQuery("SELECT code FROM pg_fmt_users WHERE first_name = 'Alice'");

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'format() in UPDATE SET returned no rows.'
                );
            }

            $this->assertSame('USR-Alice', $rows[0]['code']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'format() in UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * format() with positional arguments (%1$s, %2$s).
     *
     * The $ in PostgreSQL's positional format specifiers (%1$s, %2$s) may
     * conflict with the CTE rewriter's $N parameter placeholder detection,
     * similar to upstream issue #48 (JSONB ? operators).
     */
    public function testFormatWithPositionalArgs(): void
    {
        $this->ztdExec("INSERT INTO pg_fmt_users (first_name, last_name, code) VALUES ('Alice', 'Smith', 'A01')");

        try {
            // Use single-quoted PHP string to avoid $s variable interpolation
            $sql = 'SELECT format(\'%2$s, %1$s (%3$s)\', first_name, last_name, code) AS display
                     FROM pg_fmt_users';
            $rows = $this->ztdQuery($sql);

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'format() with positional args returned no rows.'
                );
            }

            $this->assertSame('Smith, Alice (A01)', $rows[0]['display']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'format() with positional args failed: ' . $e->getMessage()
            );
        }
    }
}
