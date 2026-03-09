<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GROUP BY with CASE expression on MySQL shadow data.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class MysqlGroupByCaseExpressionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_gbce_orders (
                id INT PRIMARY KEY,
                customer VARCHAR(100) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_gbce_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_gbce_orders VALUES (1, 'Alice', 25.00, 'completed')");
        $this->ztdExec("INSERT INTO my_gbce_orders VALUES (2, 'Bob', 150.00, 'completed')");
        $this->ztdExec("INSERT INTO my_gbce_orders VALUES (3, 'Carol', 500.00, 'completed')");
        $this->ztdExec("INSERT INTO my_gbce_orders VALUES (4, 'Dave', 75.00, 'pending')");
        $this->ztdExec("INSERT INTO my_gbce_orders VALUES (5, 'Eve', 1200.00, 'completed')");
    }

    /**
     * GROUP BY CASE expression.
     */
    public function testGroupByCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                CASE
                    WHEN amount >= 1000 THEN 'high'
                    WHEN amount >= 100 THEN 'medium'
                    ELSE 'low'
                END AS tier,
                COUNT(*) AS cnt,
                SUM(amount) AS total
             FROM my_gbce_orders
             WHERE status = 'completed'
             GROUP BY CASE
                WHEN amount >= 1000 THEN 'high'
                WHEN amount >= 100 THEN 'medium'
                ELSE 'low'
             END
             ORDER BY total DESC"
        );

        $this->assertCount(3, $rows);
        $byTier = array_column($rows, null, 'tier');
        $this->assertEquals(1, (int) $byTier['high']['cnt']);
        $this->assertEquals(2, (int) $byTier['medium']['cnt']);
        $this->assertEquals(1, (int) $byTier['low']['cnt']);
    }

    /**
     * GROUP BY CASE with HAVING.
     */
    public function testGroupByCaseWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                CASE WHEN amount >= 100 THEN 'big' ELSE 'small' END AS size,
                COUNT(*) AS cnt
             FROM my_gbce_orders
             GROUP BY CASE WHEN amount >= 100 THEN 'big' ELSE 'small' END
             HAVING COUNT(*) >= 2"
        );

        // big: Bob(150), Carol(500), Eve(1200) = 3 → passes
        // small: Alice(25), Dave(75) = 2 → passes
        $this->assertCount(2, $rows);
    }

    /**
     * GROUP BY CASE with prepared params.
     */
    public function testGroupByCaseWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT
                CASE WHEN amount >= ? THEN 'above' ELSE 'below' END AS bracket,
                COUNT(*) AS cnt
             FROM my_gbce_orders
             GROUP BY CASE WHEN amount >= ? THEN 'above' ELSE 'below' END
             ORDER BY bracket",
            [100, 100]
        );

        $this->assertCount(2, $rows);
        $byBracket = array_column($rows, null, 'bracket');
        $this->assertEquals(3, (int) $byBracket['above']['cnt']); // 150, 500, 1200
        $this->assertEquals(2, (int) $byBracket['below']['cnt']); // 25, 75
    }

    /**
     * GROUP BY CASE after shadow mutation.
     */
    public function testGroupByCaseAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO my_gbce_orders VALUES (6, 'Grace', 2000.00, 'completed')");

        $rows = $this->ztdQuery(
            "SELECT
                CASE WHEN amount >= 1000 THEN 'high' ELSE 'other' END AS tier,
                COUNT(*) AS cnt
             FROM my_gbce_orders
             WHERE status = 'completed'
             GROUP BY CASE WHEN amount >= 1000 THEN 'high' ELSE 'other' END
             ORDER BY tier"
        );

        $byTier = array_column($rows, null, 'tier');
        $this->assertEquals(2, (int) $byTier['high']['cnt']); // Eve + Grace
        $this->assertEquals(3, (int) $byTier['other']['cnt']); // Alice, Bob, Carol
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM my_gbce_orders')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
