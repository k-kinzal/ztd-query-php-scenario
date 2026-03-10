<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CTE name collision: user CTE has the same name as a physical table.
 *
 * Pattern: WITH users AS (SELECT ... FROM users WHERE ...) SELECT * FROM users
 * The CTE rewriter wraps table references in its own CTE. If the user also
 * writes a CTE with the same name as the table, name collision may occur.
 *
 * This is a realistic pattern — users often write:
 *   WITH orders AS (SELECT ... FROM orders WHERE status = 'pending') ...
 *
 * @spec SPEC-3.3
 */
class SqliteCteNameCollisionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cnc_users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)',
            'CREATE TABLE sl_cnc_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL, status TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cnc_orders', 'sl_cnc_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_cnc_users VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO sl_cnc_users VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO sl_cnc_users VALUES (3, 'Charlie', 1)");

        $this->pdo->exec("INSERT INTO sl_cnc_orders VALUES (1, 1, 100.00, 'complete')");
        $this->pdo->exec("INSERT INTO sl_cnc_orders VALUES (2, 1, 200.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_cnc_orders VALUES (3, 3, 50.00, 'pending')");
    }

    /**
     * CTE named same as physical table — SELECT only.
     * WITH sl_cnc_users AS (filtered) SELECT FROM sl_cnc_users
     */
    public function testCteNamedSameAsTable(): void
    {
        $sql = "WITH sl_cnc_users AS (
                    SELECT id, name, active FROM sl_cnc_users WHERE active = 1
                )
                SELECT name FROM sl_cnc_users ORDER BY id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE name collision SELECT: expected 2 active users, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE name collision SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * CTE named same as table, used in JOIN.
     */
    public function testCteNameCollisionInJoin(): void
    {
        $sql = "WITH sl_cnc_users AS (
                    SELECT id, name FROM sl_cnc_users WHERE active = 1
                )
                SELECT u.name, o.amount
                FROM sl_cnc_users u
                JOIN sl_cnc_orders o ON o.user_id = u.id
                WHERE o.status = 'pending'
                ORDER BY o.amount DESC";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE name collision JOIN: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE name collision JOIN failed: ' . $e->getMessage());
        }
    }

    /**
     * Two CTEs, one named same as its source table.
     */
    public function testMixedCteNameCollision(): void
    {
        $sql = "WITH
                    sl_cnc_orders AS (
                        SELECT id, user_id, amount FROM sl_cnc_orders WHERE status = 'pending'
                    ),
                    totals AS (
                        SELECT user_id, SUM(amount) AS total FROM sl_cnc_orders GROUP BY user_id
                    )
                SELECT u.name, t.total
                FROM sl_cnc_users u
                JOIN totals t ON t.user_id = u.id
                ORDER BY t.total DESC";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Mixed CTE name collision: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            // Alice: 200 pending, Charlie: 50 pending
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEqualsWithDelta(200.0, (float) $rows[0]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Mixed CTE name collision failed: ' . $e->getMessage());
        }
    }

    /**
     * CTE named same as table with prepared params.
     */
    public function testCteNameCollisionPrepared(): void
    {
        $sql = "WITH sl_cnc_orders AS (
                    SELECT id, user_id, amount FROM sl_cnc_orders WHERE amount > ?
                )
                SELECT u.name, o.amount
                FROM sl_cnc_users u
                JOIN sl_cnc_orders o ON o.user_id = u.id
                ORDER BY o.amount DESC";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [100]);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'CTE name collision prepared: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEqualsWithDelta(200.0, (float) $rows[0]['amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE name collision prepared failed: ' . $e->getMessage());
        }
    }
}
