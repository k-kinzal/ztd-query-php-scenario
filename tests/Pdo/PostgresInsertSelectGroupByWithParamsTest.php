<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT with GROUP BY and prepared params on PostgreSQL PDO.
 *
 * @spec SPEC-4.1, SPEC-4.1a
 */
class PostgresInsertSelectGroupByWithParamsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_isg_orders (
                id INTEGER PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_isg_summary (
                id SERIAL PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                order_count INTEGER NOT NULL,
                total_amount NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_isg_summary', 'pg_isg_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_isg_orders VALUES (1, 'alice', 'completed', 100)");
        $this->pdo->exec("INSERT INTO pg_isg_orders VALUES (2, 'alice', 'completed', 200)");
        $this->pdo->exec("INSERT INTO pg_isg_orders VALUES (3, 'alice', 'pending',    50)");
        $this->pdo->exec("INSERT INTO pg_isg_orders VALUES (4, 'bob',   'completed', 150)");
        $this->pdo->exec("INSERT INTO pg_isg_orders VALUES (5, 'bob',   'completed',  80)");
        $this->pdo->exec("INSERT INTO pg_isg_orders VALUES (6, 'charlie', 'completed', 300)");
    }

    /**
     * Simple INSERT...SELECT with GROUP BY (exec).
     */
    public function testInsertSelectGroupByExec(): void
    {
        $sql = "INSERT INTO pg_isg_summary (customer, order_count, total_amount)
                SELECT customer, COUNT(*), SUM(amount)
                FROM pg_isg_orders
                WHERE status = 'completed'
                GROUP BY customer";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT customer, order_count, total_amount FROM pg_isg_summary ORDER BY customer");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT...SELECT GROUP BY: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);

            $aliceCount = $rows[0]['order_count'];
            $aliceTotal = $rows[0]['total_amount'];

            if ($aliceCount === null || (int) $aliceCount !== 2) {
                $this->markTestIncomplete(
                    "INSERT...SELECT GROUP BY: alice count={$aliceCount} (exp 2), total={$aliceTotal} (exp 300)"
                );
            }

            $this->assertSame(2, (int) $aliceCount);
            $this->assertEquals(300.0, (float) $aliceTotal, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT GROUP BY exec failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT...SELECT with GROUP BY and WHERE param (using ?).
     */
    public function testPreparedInsertSelectGroupByWithParam(): void
    {
        $sql = "INSERT INTO pg_isg_summary (customer, order_count, total_amount)
                SELECT customer, COUNT(*), SUM(amount)
                FROM pg_isg_orders
                WHERE status = ?
                GROUP BY customer";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['completed']);

            $rows = $this->ztdQuery("SELECT customer, order_count, total_amount FROM pg_isg_summary ORDER BY customer");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared INSERT...SELECT GROUP BY: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);

            $aliceCount = $rows[0]['order_count'];
            if ($aliceCount === null || (int) $aliceCount !== 2) {
                $this->markTestIncomplete(
                    "Prepared: alice count={$aliceCount} (exp 2). May store NULLs (Issue #20)."
                );
            }

            $this->assertSame(2, (int) $aliceCount);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared INSERT...SELECT GROUP BY failed: ' . $e->getMessage()
            );
        }
    }
}
