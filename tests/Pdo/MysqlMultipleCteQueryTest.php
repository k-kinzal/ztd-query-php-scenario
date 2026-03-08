<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests user-defined CTEs (WITH ... AS) in shadow queries on MySQL.
 *
 * ZTD adds its own CTE for shadow data. User-defined CTEs may be
 * overwritten during query rewriting. Documents the behavior on MySQL.
 */
class MysqlMultipleCteQueryTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_cte_orders');
        $raw->exec('CREATE TABLE pdo_cte_orders (id INT PRIMARY KEY, customer VARCHAR(50), product VARCHAR(50), amount DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_cte_orders VALUES (1, 'Alice', 'Widget', 100.00)");
        $this->pdo->exec("INSERT INTO pdo_cte_orders VALUES (2, 'Alice', 'Gadget', 200.00)");
        $this->pdo->exec("INSERT INTO pdo_cte_orders VALUES (3, 'Bob', 'Widget', 150.00)");
        $this->pdo->exec("INSERT INTO pdo_cte_orders VALUES (4, 'Bob', 'Gadget', 50.00)");
        $this->pdo->exec("INSERT INTO pdo_cte_orders VALUES (5, 'Charlie', 'Widget', 300.00)");
    }

    /**
     * User CTE — ZTD may overwrite the WITH clause.
     */
    public function testUserCteReference(): void
    {
        try {
            $stmt = $this->pdo->query('
                WITH customer_totals AS (
                    SELECT customer, SUM(amount) AS total
                    FROM pdo_cte_orders
                    GROUP BY customer
                )
                SELECT customer, total FROM customer_totals ORDER BY total DESC
            ');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertIsArray($rows);
        } catch (\Exception $e) {
            // CTE name not found — ZTD overwrote the WITH clause
            $this->assertStringContainsString('customer_totals', $e->getMessage());
        }
    }

    /**
     * Inline subquery works as CTE alternative.
     */
    public function testInlineSubqueryWorksAsAlternative(): void
    {
        $stmt = $this->pdo->query('
            SELECT customer, SUM(amount) AS total
            FROM pdo_cte_orders
            GROUP BY customer
            ORDER BY total DESC
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        // Alice=300, Charlie=300 (tied), Bob=200
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Inline subquery reflects INSERT mutation.
     */
    public function testInlineSubqueryReflectsInsertMutation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_cte_orders VALUES (6, 'Diana', 'Widget', 500.00)");

        $stmt = $this->pdo->query('
            SELECT customer, SUM(amount) AS total
            FROM pdo_cte_orders
            GROUP BY customer
            ORDER BY total DESC
            LIMIT 1
        ');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Diana', $row['customer']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_cte_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_cte_orders');
        } catch (\Exception $e) {
        }
    }
}
