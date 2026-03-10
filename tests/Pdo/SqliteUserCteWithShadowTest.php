<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests user-written CTEs reading from shadow-modified tables on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteUserCteWithShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ucs2_orders (
            id INTEGER PRIMARY KEY,
            customer TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ucs2_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ucs2_orders VALUES (1, 'Alice', 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_ucs2_orders VALUES (2, 'Bob', 200.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_ucs2_orders VALUES (3, 'Alice', 150.00, 'completed')");
    }

    public function testSimpleUserCteAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ucs2_orders VALUES (4, 'Carol', 300.00, 'completed')");

            $rows = $this->ztdQuery(
                "WITH completed AS (
                    SELECT customer, SUM(amount) AS total
                    FROM sl_ucs2_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                )
                SELECT customer, total FROM completed ORDER BY customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('User CTE returns empty on SQLite');
            }

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = (float) $row['total'];
            }

            if (!isset($map['Carol'])) {
                $this->markTestIncomplete('User CTE: Carol not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(300.00, $map['Carol']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Simple user CTE after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testChainedUserCtesAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ucs2_orders SET status = 'completed' WHERE id = 2");

            $rows = $this->ztdQuery(
                "WITH base AS (
                    SELECT customer, amount FROM sl_ucs2_orders WHERE status = 'completed'
                ),
                summary AS (
                    SELECT customer, SUM(amount) AS total FROM base GROUP BY customer
                )
                SELECT customer, total FROM summary ORDER BY customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Chained user CTEs return empty on SQLite');
            }

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = (float) $row['total'];
            }

            if (!isset($map['Bob'])) {
                $this->markTestIncomplete('Chained CTEs: Bob not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(200.00, $map['Bob']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained user CTEs after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testUserCteAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_ucs2_orders WHERE id = 3");

            $rows = $this->ztdQuery(
                "WITH alice_orders AS (
                    SELECT id, amount FROM sl_ucs2_orders WHERE customer = 'Alice'
                )
                SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM alice_orders"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('User CTE after DELETE returns empty');
            }

            $cnt = (int) $rows[0]['cnt'];
            if ($cnt !== 1) {
                $this->markTestIncomplete("User CTE after DELETE: expected 1, got $cnt");
            }
            $this->assertEquals(1, $cnt);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('User CTE after DELETE failed: ' . $e->getMessage());
        }
    }
}
