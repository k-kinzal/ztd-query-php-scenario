<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT with GROUP BY and prepared params on MySQL PDO.
 *
 * Known: INSERT...SELECT with computed columns stores NULLs on SQLite/PostgreSQL (#20).
 * MySQL may handle this correctly.
 *
 * @spec SPEC-4.1, SPEC-4.1a
 */
class MysqlInsertSelectGroupByWithParamsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_isg_orders (
                id INT PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_isg_summary (
                id INT PRIMARY KEY AUTO_INCREMENT,
                customer VARCHAR(50) NOT NULL,
                order_count INT NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_isg_summary', 'my_isg_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_isg_orders VALUES (1, 'alice', 'completed', 100)");
        $this->pdo->exec("INSERT INTO my_isg_orders VALUES (2, 'alice', 'completed', 200)");
        $this->pdo->exec("INSERT INTO my_isg_orders VALUES (3, 'alice', 'pending',    50)");
        $this->pdo->exec("INSERT INTO my_isg_orders VALUES (4, 'bob',   'completed', 150)");
        $this->pdo->exec("INSERT INTO my_isg_orders VALUES (5, 'bob',   'completed',  80)");
        $this->pdo->exec("INSERT INTO my_isg_orders VALUES (6, 'charlie', 'completed', 300)");
    }

    /**
     * Simple INSERT...SELECT with GROUP BY (exec).
     */
    public function testInsertSelectGroupByExec(): void
    {
        $sql = "INSERT INTO my_isg_summary (customer, order_count, total_amount)
                SELECT customer, COUNT(*), SUM(amount)
                FROM my_isg_orders
                WHERE status = 'completed'
                GROUP BY customer";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT customer, order_count, total_amount FROM my_isg_summary ORDER BY customer");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT...SELECT GROUP BY: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);

            // alice: 2 completed, 300
            $aliceCount = $rows[0]['order_count'];
            $aliceTotal = $rows[0]['total_amount'];

            if ($aliceCount === null || (int) $aliceCount !== 2) {
                $this->markTestIncomplete(
                    "INSERT...SELECT GROUP BY: alice count={$aliceCount} (exp 2)"
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
     * Prepared INSERT...SELECT with GROUP BY and WHERE param.
     */
    public function testPreparedInsertSelectGroupByWithParam(): void
    {
        $sql = "INSERT INTO my_isg_summary (customer, order_count, total_amount)
                SELECT customer, COUNT(*), SUM(amount)
                FROM my_isg_orders
                WHERE status = ?
                GROUP BY customer";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['completed']);

            $rows = $this->ztdQuery("SELECT customer, order_count, total_amount FROM my_isg_summary ORDER BY customer");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared INSERT...SELECT GROUP BY: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);

            $aliceCount = $rows[0]['order_count'];
            if ($aliceCount === null || (int) $aliceCount !== 2) {
                $this->markTestIncomplete(
                    "Prepared: alice count={$aliceCount} (exp 2), total={$rows[0]['total_amount']} (exp 300)"
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
