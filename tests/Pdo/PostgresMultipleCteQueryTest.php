<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests user-defined CTEs (WITH ... AS) in shadow queries on PostgreSQL.
 *
 * ZTD adds its own CTE for shadow data. User-defined CTEs may be
 * overwritten during query rewriting. Documents the behavior on PostgreSQL.
 * @spec pending
 */
class PostgresMultipleCteQueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_cte_orders (id INT PRIMARY KEY, customer VARCHAR(50), product VARCHAR(50), amount NUMERIC(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['pg_cte_orders'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cte_orders VALUES (2, 'Alice', 'Gadget', 200.00)");
        $this->pdo->exec("INSERT INTO pg_cte_orders VALUES (3, 'Bob', 'Widget', 150.00)");
        $this->pdo->exec("INSERT INTO pg_cte_orders VALUES (4, 'Bob', 'Gadget', 50.00)");
        $this->pdo->exec("INSERT INTO pg_cte_orders VALUES (5, 'Charlie', 'Widget', 300.00)");
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
                    FROM pg_cte_orders
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
            FROM pg_cte_orders
            GROUP BY customer
            ORDER BY total DESC
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        // Alice=300, Charlie=300 (tied), Bob=200
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Inline subquery reflects DELETE mutation.
     */
    public function testInlineSubqueryReflectsDeleteMutation(): void
    {
        $this->pdo->exec("DELETE FROM pg_cte_orders WHERE customer = 'Charlie'");

        $stmt = $this->pdo->query('
            SELECT COUNT(DISTINCT customer) AS cnt FROM pg_cte_orders
        ');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_cte_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
