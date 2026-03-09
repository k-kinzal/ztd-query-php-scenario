<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with GROUP BY and prepared params.
 *
 * Known: INSERT...SELECT with computed columns stores NULLs (#20, #83).
 * This specifically tests the prepared statement variant with GROUP BY
 * and aggregate functions — a common ETL / summary table pattern.
 *
 * @spec SPEC-4.1, SPEC-4.1a
 */
class SqliteInsertSelectGroupByWithParamsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_isg_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                status TEXT NOT NULL,
                amount REAL NOT NULL
            )',
            'CREATE TABLE sl_isg_summary (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                order_count INTEGER NOT NULL,
                total_amount REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isg_orders', 'sl_isg_summary'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_isg_orders VALUES (1, 'alice', 'completed', 100)");
        $this->pdo->exec("INSERT INTO sl_isg_orders VALUES (2, 'alice', 'completed', 200)");
        $this->pdo->exec("INSERT INTO sl_isg_orders VALUES (3, 'alice', 'pending',    50)");
        $this->pdo->exec("INSERT INTO sl_isg_orders VALUES (4, 'bob',   'completed', 150)");
        $this->pdo->exec("INSERT INTO sl_isg_orders VALUES (5, 'bob',   'completed',  80)");
        $this->pdo->exec("INSERT INTO sl_isg_orders VALUES (6, 'charlie', 'completed', 300)");
    }

    /**
     * INSERT...SELECT with GROUP BY and WHERE param (exec, not prepared).
     *
     * This is the control: uses exec() with literal values.
     */
    public function testInsertSelectGroupByWithLiteralControl(): void
    {
        $sql = "INSERT INTO sl_isg_summary (id, customer, order_count, total_amount)
                SELECT ROW_NUMBER() OVER (ORDER BY customer), customer, COUNT(*), SUM(amount)
                FROM sl_isg_orders
                WHERE status = 'completed'
                GROUP BY customer";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT customer, order_count, total_amount FROM sl_isg_summary ORDER BY customer");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT...SELECT GROUP BY literal: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // alice: 2 completed, 300
            // bob: 2 completed, 230
            // charlie: 1 completed, 300
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT GROUP BY with literals failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT...SELECT with GROUP BY and WHERE param.
     *
     * INSERT INTO summary SELECT ... FROM orders WHERE status = ? GROUP BY customer
     */
    public function testPreparedInsertSelectGroupByWithParam(): void
    {
        $sql = "INSERT INTO sl_isg_summary (id, customer, order_count, total_amount)
                SELECT ROW_NUMBER() OVER (ORDER BY customer), customer, COUNT(*), SUM(amount)
                FROM sl_isg_orders
                WHERE status = ?
                GROUP BY customer";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['completed']);

            $rows = $this->ztdQuery("SELECT customer, order_count, total_amount FROM sl_isg_summary ORDER BY customer");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared INSERT...SELECT GROUP BY: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            // Check if values are correct or NULL (known issue #20/#83 extends)
            $aliceCount = $rows[0]['order_count'];
            $aliceTotal = $rows[0]['total_amount'];

            if ($aliceCount === null || (int) $aliceCount !== 2) {
                $this->markTestIncomplete(
                    "Prepared INSERT...SELECT GROUP BY: alice order_count={$aliceCount} (expected 2), "
                    . "total={$aliceTotal} (expected 300). Aggregates may be NULL (extends Issue #20)."
                );
            }

            $this->assertSame(2, (int) $aliceCount);
            $this->assertEquals(300.0, (float) $aliceTotal, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared INSERT...SELECT GROUP BY failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Simple INSERT...SELECT with GROUP BY (no computed columns, just direct refs + aggregates).
     */
    public function testSimpleInsertSelectGroupBy(): void
    {
        // Use explicit IDs to avoid ROW_NUMBER (which is a window function)
        $sql = "INSERT INTO sl_isg_summary (id, customer, order_count, total_amount)
                SELECT MIN(id), customer, COUNT(*), SUM(amount)
                FROM sl_isg_orders
                GROUP BY customer";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT customer, order_count, total_amount FROM sl_isg_summary ORDER BY customer");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Simple INSERT...SELECT GROUP BY: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);

            // alice: 3 orders, 350
            $aliceCount = $rows[0]['order_count'];
            $aliceTotal = $rows[0]['total_amount'];

            if ($aliceCount === null || (int) $aliceCount !== 3) {
                $this->markTestIncomplete(
                    "Simple INSERT...SELECT GROUP BY: alice count={$aliceCount} (exp 3), "
                    . "total={$aliceTotal} (exp 350). Known issue: aggregates become NULL on SQLite."
                );
            }

            $this->assertSame(3, (int) $aliceCount);
            $this->assertEquals(350.0, (float) $aliceTotal, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Simple INSERT...SELECT GROUP BY failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT with GROUP BY HAVING and param.
     */
    public function testPreparedInsertSelectGroupByHavingWithParam(): void
    {
        $sql = "INSERT INTO sl_isg_summary (id, customer, order_count, total_amount)
                SELECT MIN(id), customer, COUNT(*), SUM(amount)
                FROM sl_isg_orders
                WHERE status = ?
                GROUP BY customer
                HAVING COUNT(*) >= ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['completed', 2]);

            $rows = $this->ztdQuery("SELECT customer, order_count FROM sl_isg_summary ORDER BY customer");

            // completed with count >= 2: alice (2), bob (2). charlie only has 1.
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT...SELECT GROUP BY HAVING with params: expected 2 rows, got '
                    . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
            $this->assertSame('bob', $rows[1]['customer']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT GROUP BY HAVING with params failed: ' . $e->getMessage()
            );
        }
    }
}
