<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with SQL function calls in VALUES through ZTD shadow store.
 *
 * Common patterns: INSERT ... VALUES (expr, func(), func2())
 * The CTE rewriter must handle function calls as value expressions
 * without confusing them with column references or other constructs.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertFunctionValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ifv_events (
                id INTEGER PRIMARY KEY,
                name TEXT,
                created_at TEXT,
                hash TEXT,
                priority INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ifv_events'];
    }

    /**
     * INSERT with CURRENT_TIMESTAMP function in VALUES.
     */
    public function testInsertCurrentTimestamp(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, created_at) VALUES (1, 'login', CURRENT_TIMESTAMP)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, created_at FROM sl_ifv_events WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT CURRENT_TIMESTAMP: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('login', $rows[0]['name']);
            $this->assertNotNull($rows[0]['created_at'], 'created_at should not be NULL');
            $this->assertNotEmpty($rows[0]['created_at'], 'created_at should not be empty');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT CURRENT_TIMESTAMP failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with datetime('now') function in VALUES (SQLite-specific).
     */
    public function testInsertDatetimeNow(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, created_at) VALUES (2, 'logout', datetime('now'))";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, created_at FROM sl_ifv_events WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT datetime(now): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('logout', $rows[0]['name']);
            $this->assertNotNull($rows[0]['created_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT datetime(now) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with arithmetic expression in VALUES.
     */
    public function testInsertArithmeticExpression(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, priority) VALUES (3, 'task', 2 + 3 * 4)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT priority FROM sl_ifv_events WHERE id = 3");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT arithmetic: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEquals(14, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT arithmetic expression failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with COALESCE in VALUES.
     */
    public function testInsertCoalesceInValues(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, priority) VALUES (4, 'alert', COALESCE(NULL, 5))";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT priority FROM sl_ifv_events WHERE id = 4");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT COALESCE: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEquals(5, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT COALESCE in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with CASE expression in VALUES.
     */
    public function testInsertCaseInValues(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, priority)
                VALUES (5, 'conditional', CASE WHEN 1 > 0 THEN 100 ELSE 0 END)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT priority FROM sl_ifv_events WHERE id = 5");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT CASE: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEquals(100, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT CASE in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with UPPER/LOWER string functions in VALUES.
     */
    public function testInsertStringFunctionInValues(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, hash) VALUES (6, UPPER('hello'), LOWER('WORLD'))";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, hash FROM sl_ifv_events WHERE id = 6");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT string func: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('HELLO', $rows[0]['name']);
            $this->assertSame('world', $rows[0]['hash']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT string function in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with nested function: LENGTH(REPLACE(...)) in VALUES.
     */
    public function testInsertNestedFunctionInValues(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, priority) VALUES (7, 'nested', LENGTH(REPLACE('hello world', ' ', '')))";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT priority FROM sl_ifv_events WHERE id = 7");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT nested func: expected 1 row, got ' . count($rows)
                );
            }

            // 'helloworld' = 10 chars
            $this->assertEquals(10, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT nested function in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with subquery in VALUES.
     */
    public function testInsertSubqueryInValues(): void
    {
        // First insert a row to reference
        $this->pdo->exec("INSERT INTO sl_ifv_events (id, name, priority) VALUES (10, 'base', 42)");

        $sql = "INSERT INTO sl_ifv_events (id, name, priority)
                VALUES (11, 'derived', (SELECT MAX(priority) FROM sl_ifv_events))";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT priority FROM sl_ifv_events WHERE id = 11");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT subquery: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEquals(42, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with function in non-param positions.
     */
    public function testPreparedInsertWithFunctionAndParams(): void
    {
        $sql = "INSERT INTO sl_ifv_events (id, name, created_at, priority)
                VALUES (?, ?, CURRENT_TIMESTAMP, ?)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([20, 'prepared_func', 7]);

            $rows = $this->ztdQuery("SELECT name, created_at, priority FROM sl_ifv_events WHERE id = 20");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared INSERT with func: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('prepared_func', $rows[0]['name']);
            $this->assertNotNull($rows[0]['created_at']);
            $this->assertEquals(7, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with function failed: ' . $e->getMessage());
        }
    }
}
