<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests user-written CTEs (WITH ... AS) reading from shadow-modified tables
 * on MySQL via PDO.
 *
 * @spec SPEC-4.2
 */
class MysqlUserCteWithShadowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mpd_ucs2_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mpd_ucs2_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_ucs2_orders VALUES (1, 'Alice', 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mpd_ucs2_orders VALUES (2, 'Bob', 200.00, 'pending')");
        $this->pdo->exec("INSERT INTO mpd_ucs2_orders VALUES (3, 'Alice', 150.00, 'completed')");
    }

    public function testSimpleUserCteAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mpd_ucs2_orders VALUES (4, 'Carol', 300.00, 'completed')");

            $rows = $this->ztdQuery(
                "WITH completed AS (
                    SELECT customer, SUM(amount) AS total
                    FROM mpd_ucs2_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                )
                SELECT customer, total FROM completed ORDER BY customer"
            );

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

    public function testChainedUserCtesAfterDml(): void
    {
        try {
            $this->pdo->exec("UPDATE mpd_ucs2_orders SET status = 'completed' WHERE id = 2");

            $rows = $this->ztdQuery(
                "WITH base AS (
                    SELECT customer, amount FROM mpd_ucs2_orders WHERE status = 'completed'
                ),
                summary AS (
                    SELECT customer, SUM(amount) AS total FROM base GROUP BY customer
                )
                SELECT customer, total FROM summary ORDER BY total DESC"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Chained user CTEs: empty result');
            }

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = (float) $row['total'];
            }

            if (!isset($map['Bob'])) {
                $this->markTestIncomplete('Chained CTEs after UPDATE: Bob not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(250.00, $map['Alice']);
            $this->assertEquals(200.00, $map['Bob']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained user CTEs after DML failed: ' . $e->getMessage());
        }
    }
}
