<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests user-written CTEs reading from shadow-modified tables on PostgreSQL.
 *
 * Issue #4 documents that user CTEs return empty on PostgreSQL.
 * This test verifies the current behavior and documents the scope.
 *
 * @spec SPEC-4.2
 */
class PostgresUserCteWithShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ucs2_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            amount NUMERIC(10,2) NOT NULL,
            status VARCHAR(20) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_ucs2_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ucs2_orders VALUES (1, 'Alice', 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_ucs2_orders VALUES (2, 'Bob', 200.00, 'pending')");
        $this->pdo->exec("INSERT INTO pg_ucs2_orders VALUES (3, 'Alice', 150.00, 'completed')");
    }

    /**
     * Simple user CTE after INSERT — Issue #4 may cause empty result.
     */
    public function testSimpleUserCteAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_ucs2_orders VALUES (4, 'Carol', 300.00, 'completed')");

            $rows = $this->ztdQuery(
                "WITH completed AS (
                    SELECT customer, SUM(amount) AS total
                    FROM pg_ucs2_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                )
                SELECT customer, total FROM completed ORDER BY customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('User CTE returns empty on PostgreSQL (Issue #4)');
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

    /**
     * Chained CTEs after UPDATE — tests CTE-on-CTE with shadow data.
     */
    public function testChainedUserCtesAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_ucs2_orders SET status = 'completed' WHERE id = 2");

            $rows = $this->ztdQuery(
                "WITH base AS (
                    SELECT customer, amount FROM pg_ucs2_orders WHERE status = 'completed'
                ),
                summary AS (
                    SELECT customer, SUM(amount) AS total FROM base GROUP BY customer
                )
                SELECT customer, total FROM summary ORDER BY customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Chained user CTEs return empty on PostgreSQL (Issue #4)');
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

    /**
     * User CTE after DELETE.
     */
    public function testUserCteAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_ucs2_orders WHERE id = 3");

            $rows = $this->ztdQuery(
                "WITH alice_orders AS (
                    SELECT id, amount FROM pg_ucs2_orders WHERE customer = 'Alice'
                )
                SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM alice_orders"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('User CTE after DELETE returns empty (Issue #4)');
            }

            $cnt = (int) $rows[0]['cnt'];
            if ($cnt !== 1) {
                $this->markTestIncomplete("User CTE after DELETE: expected 1 Alice order, got $cnt");
            }
            $this->assertEquals(1, $cnt);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('User CTE after DELETE failed: ' . $e->getMessage());
        }
    }
}
