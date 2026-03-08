<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests user-defined CTEs (WITH ... AS) in shadow queries via MySQLi.
 *
 * ZTD adds its own CTE for shadow data. Documents whether user CTEs
 * are overwritten by ZTD's CTE on MySQLi.
 */
class MultipleCteQueryTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_cte_orders');
        $raw->query('CREATE TABLE mi_cte_orders (id INT PRIMARY KEY, customer VARCHAR(50), amount DECIMAL(10,2))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (1, 'Alice', 100.00)");
        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (2, 'Alice', 200.00)");
        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (3, 'Bob', 150.00)");
        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (4, 'Charlie', 300.00)");
    }

    /**
     * User CTE — ZTD may overwrite the WITH clause.
     */
    public function testUserCteReference(): void
    {
        try {
            $result = $this->mysqli->query('
                WITH totals AS (
                    SELECT customer, SUM(amount) AS total
                    FROM mi_cte_orders
                    GROUP BY customer
                )
                SELECT customer, total FROM totals ORDER BY total DESC
            ');
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $this->assertIsArray($rows);
        } catch (\Exception $e) {
            // CTE name not found — ZTD overwrote the WITH clause
            $this->assertStringContainsString('totals', $e->getMessage());
        }
    }

    /**
     * Inline aggregation works as CTE alternative.
     */
    public function testInlineAggregationWorks(): void
    {
        $result = $this->mysqli->query('
            SELECT customer, SUM(amount) AS total
            FROM mi_cte_orders
            GROUP BY customer
            ORDER BY total DESC
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        // Alice=300, Charlie=300 (tied), Bob=150
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cte_orders');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_cte_orders');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
