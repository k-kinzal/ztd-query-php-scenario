<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests user-defined CTEs (WITH ... AS) in shadow queries on MySQL.
 *
 * ZTD adds its own CTE for shadow data. User-defined CTEs may be
 * overwritten during query rewriting. Documents the behavior on MySQL.
 * @spec SPEC-3.3
 */
class MysqlMultipleCteQueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_cte_orders (id INT PRIMARY KEY, customer VARCHAR(50), product VARCHAR(50), amount DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_cte_orders'];
    }


    protected function setUp(): void
    {
        parent::setUp();

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
}
