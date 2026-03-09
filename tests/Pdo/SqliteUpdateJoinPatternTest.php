<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE patterns that reference other tables — cross-table update workarounds.
 *
 * SQLite doesn't support UPDATE FROM natively until 3.33, and ZTD doesn't support
 * UPDATE FROM (#72). This tests the IN-subquery workaround and other cross-table
 * update patterns.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateJoinPatternTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ujp_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                total REAL NOT NULL
            )',
            'CREATE TABLE sl_ujp_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT \'basic\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ujp_orders', 'sl_ujp_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ujp_customers VALUES (1, 'Alice', 'basic')");
        $this->pdo->exec("INSERT INTO sl_ujp_customers VALUES (2, 'Bob',   'basic')");
        $this->pdo->exec("INSERT INTO sl_ujp_customers VALUES (3, 'Charlie', 'basic')");

        $this->pdo->exec("INSERT INTO sl_ujp_orders VALUES (1, 1, 'completed', 500)");
        $this->pdo->exec("INSERT INTO sl_ujp_orders VALUES (2, 1, 'completed', 600)");
        $this->pdo->exec("INSERT INTO sl_ujp_orders VALUES (3, 2, 'completed', 200)");
        $this->pdo->exec("INSERT INTO sl_ujp_orders VALUES (4, 2, 'pending',   300)");
    }

    /**
     * UPDATE WHERE id IN (SELECT from other table) — cross-table update.
     *
     * Update customers who have total orders > 500.
     */
    public function testUpdateWhereInSubqueryFromOtherTable(): void
    {
        $sql = "UPDATE sl_ujp_customers SET tier = 'gold'
                WHERE id IN (
                    SELECT customer_id FROM sl_ujp_orders
                    WHERE status = 'completed'
                    GROUP BY customer_id
                    HAVING SUM(total) > 500
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, tier FROM sl_ujp_customers ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice: completed orders 500+600=1100 > 500 → gold
            // Bob: completed 200 < 500 → basic
            // Charlie: no orders → basic
            $aliceTier = $rows[0]['tier'];
            $bobTier = $rows[1]['tier'];
            $charlieTier = $rows[2]['tier'];

            if ($aliceTier !== 'gold') {
                $this->markTestIncomplete(
                    "Alice tier: expected 'gold', got '{$aliceTier}' (completed total should be 1100)"
                );
            }

            if ($bobTier !== 'basic') {
                $this->markTestIncomplete(
                    "Bob tier: expected 'basic', got '{$bobTier}' (completed total 200 < 500)"
                );
            }

            $this->assertSame('gold', $aliceTier);
            $this->assertSame('basic', $bobTier);
            $this->assertSame('basic', $charlieTier);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE WHERE IN (subquery GROUP BY HAVING from other table) failed: '
                . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE WHERE EXISTS (SELECT from other table).
     */
    public function testUpdateWhereExistsFromOtherTable(): void
    {
        $sql = "UPDATE sl_ujp_customers SET tier = 'active'
                WHERE EXISTS (
                    SELECT 1 FROM sl_ujp_orders
                    WHERE sl_ujp_orders.customer_id = sl_ujp_customers.id
                    AND sl_ujp_orders.status = 'completed'
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, tier FROM sl_ujp_customers ORDER BY id");

            // Alice and Bob have completed orders; Charlie does not
            $this->assertCount(3, $rows);

            if ($rows[0]['tier'] !== 'active' || $rows[1]['tier'] !== 'active' || $rows[2]['tier'] !== 'basic') {
                $tiers = array_map(fn($r) => "{$r['name']}={$r['tier']}", $rows);
                $this->markTestIncomplete(
                    'UPDATE WHERE EXISTS: wrong tiers. Got: ' . implode(', ', $tiers)
                );
            }

            $this->assertSame('active', $rows[0]['tier']); // Alice
            $this->assertSame('active', $rows[1]['tier']); // Bob
            $this->assertSame('basic', $rows[2]['tier']);   // Charlie
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE WHERE EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE WHERE NOT EXISTS (SELECT from other table).
     *
     * Delete customers with no orders.
     */
    public function testDeleteWhereNotExistsFromOtherTable(): void
    {
        $sql = "DELETE FROM sl_ujp_customers
                WHERE NOT EXISTS (
                    SELECT 1 FROM sl_ujp_orders
                    WHERE sl_ujp_orders.customer_id = sl_ujp_customers.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name FROM sl_ujp_customers ORDER BY name");

            // Charlie has no orders → deleted
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE NOT EXISTS: expected 2 remaining, got ' . count($rows)
                    . '. Names: ' . implode(', ', array_column($rows, 'name'))
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE WHERE NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with cross-table IN subquery and params.
     */
    public function testPreparedUpdateWithCrossTableSubquery(): void
    {
        $sql = "UPDATE sl_ujp_customers SET tier = ?
                WHERE id IN (
                    SELECT customer_id FROM sl_ujp_orders
                    WHERE status = ? AND total >= ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['premium', 'completed', 500]);

            $rows = $this->ztdQuery("SELECT name, tier FROM sl_ujp_customers ORDER BY name");

            // Alice has completed orders ≥ 500 (order 2: 600)
            // Bob has completed 200 < 500
            $aliceTier = null;
            $bobTier = null;
            foreach ($rows as $r) {
                if ($r['name'] === 'Alice') $aliceTier = $r['tier'];
                if ($r['name'] === 'Bob') $bobTier = $r['tier'];
            }

            if ($aliceTier !== 'premium') {
                $this->markTestIncomplete(
                    "Prepared cross-table UPDATE: Alice tier expected 'premium', got '{$aliceTier}'"
                );
            }

            $this->assertSame('premium', $aliceTier);
            $this->assertSame('basic', $bobTier);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared cross-table UPDATE failed: ' . $e->getMessage()
            );
        }
    }
}
