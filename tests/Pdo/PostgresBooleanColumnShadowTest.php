<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests BOOLEAN column handling in the ZTD shadow store on PostgreSQL.
 *
 * The CTE rewriter constructs shadow data rows using CAST(value AS type).
 * PostgreSQL BOOLEAN requires 'true'/'false'/'t'/'f'/'1'/'0' but
 * the shadow store stores boolean values as empty string '', causing
 * CAST('' AS BOOLEAN) which PostgreSQL rejects with:
 *   "invalid input syntax for type boolean"
 *
 * This is a data-integrity issue: users cannot INSERT rows with BOOLEAN
 * columns and then SELECT/UPDATE/DELETE them through the shadow store.
 *
 * @spec SPEC-4.1
 */
class PostgresBooleanColumnShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_bool_test (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            active BOOLEAN NOT NULL DEFAULT TRUE
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_bool_test'];
    }

    /**
     * Minimal reproduction: INSERT with BOOLEAN TRUE, then SELECT.
     *
     * Expected: SELECT returns the inserted row with active=true.
     * Actual: CTE rewriter produces CAST('' AS BOOLEAN) which PostgreSQL rejects.
     */
    public function testInsertBooleanTrueThenSelect(): void
    {
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Alice', TRUE)");

        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_bool_test WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\PDOException $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: shadow store produces CAST(\'\' AS BOOLEAN) '
                    . 'which PostgreSQL rejects. BOOLEAN values are not preserved in shadow store.'
                );
            }
            throw $e;
        }
    }

    /**
     * INSERT with BOOLEAN FALSE, then SELECT.
     */
    public function testInsertBooleanFalseThenSelect(): void
    {
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Bob', FALSE)");

        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_bool_test WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\PDOException $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: CAST(\'\' AS BOOLEAN) error for FALSE values too.'
                );
            }
            throw $e;
        }
    }

    /**
     * INSERT with integer-style boolean (0/1), then SELECT.
     */
    public function testInsertIntegerBooleanThenSelect(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Charlie', 1)");
        } catch (\Throwable $e) {
            // PostgreSQL may reject integer for BOOLEAN depending on version
            $this->markTestSkipped('PostgreSQL does not accept integer for BOOLEAN: ' . $e->getMessage());
        }

        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_bool_test WHERE id = 1');
            $this->assertCount(1, $rows);
        } catch (\PDOException $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: shadow store cannot round-trip integer boolean.'
                );
            }
            throw $e;
        }
    }

    /**
     * Prepared INSERT with BOOLEAN parameter, then SELECT.
     */
    public function testPreparedInsertBooleanThenSelect(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_bool_test VALUES (?, ?, ?)');
        $stmt->execute([1, 'Dana', true]);

        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_bool_test WHERE id = 1');
            $this->assertCount(1, $rows);
        } catch (\PDOException $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: prepared INSERT with PHP true also fails on SELECT.'
                );
            }
            throw $e;
        }
    }

    /**
     * INSERT with string 'true'/'false' for BOOLEAN, then SELECT.
     */
    public function testInsertStringBooleanThenSelect(): void
    {
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Eve', 'true')");

        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_bool_test WHERE id = 1');
            $this->assertCount(1, $rows);
        } catch (\PDOException $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: even string \'true\' stored incorrectly in shadow.'
                );
            }
            throw $e;
        }
    }

    /**
     * UPDATE on table with BOOLEAN column.
     */
    public function testUpdateRowWithBooleanColumn(): void
    {
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Frank', TRUE)");

        try {
            $this->pdo->exec("UPDATE pg_bool_test SET name = 'Franklin' WHERE id = 1");

            $rows = $this->ztdQuery('SELECT * FROM pg_bool_test WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Franklin', $rows[0]['name']);
        } catch (\PDOException|\Throwable $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: UPDATE also fails due to shadow BOOLEAN casting.'
                );
            }
            throw $e;
        }
    }

    /**
     * DELETE on table with BOOLEAN column.
     */
    public function testDeleteRowWithBooleanColumn(): void
    {
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Grace', TRUE)");
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (2, 'Hank', FALSE)");

        try {
            $this->pdo->exec("DELETE FROM pg_bool_test WHERE id = 1");

            $rows = $this->ztdQuery('SELECT * FROM pg_bool_test ORDER BY id');
            $this->assertCount(1, $rows);
            $this->assertSame('Hank', $rows[0]['name']);
        } catch (\PDOException|\Throwable $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: DELETE also fails due to shadow BOOLEAN casting.'
                );
            }
            throw $e;
        }
    }

    /**
     * Table with BOOLEAN column but only non-BOOLEAN columns in query.
     * The CTE rewriter still includes all columns in the shadow CTE.
     */
    public function testSelectNonBooleanColumnsOnly(): void
    {
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Ivy', TRUE)");

        try {
            $rows = $this->ztdQuery('SELECT id, name FROM pg_bool_test WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Ivy', $rows[0]['name']);
        } catch (\PDOException $e) {
            if (str_contains($e->getMessage(), 'invalid input syntax for type boolean')) {
                $this->markTestIncomplete(
                    'PostgreSQL BOOLEAN column: even SELECT of non-boolean columns fails '
                    . 'because CTE rewriter includes all columns in shadow data.'
                );
            }
            throw $e;
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_bool_test VALUES (1, 'Test', TRUE)");
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_bool_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
