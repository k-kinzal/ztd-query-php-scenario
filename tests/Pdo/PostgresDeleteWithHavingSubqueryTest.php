<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE operations where the target rows are determined by
 * subqueries involving GROUP BY and HAVING through PostgreSQL CTE rewriter.
 *
 * @spec SPEC-4.3
 */
class PostgresDeleteWithHavingSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dh_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            )',
            'CREATE TABLE pg_dh_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                tier VARCHAR(10) NOT NULL DEFAULT \'standard\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dh_orders', 'pg_dh_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_dh_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO pg_dh_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO pg_dh_customers VALUES (3, 'Carol', 'standard')");
        $this->pdo->exec("INSERT INTO pg_dh_customers VALUES (4, 'Dave', 'premium')");

        $this->pdo->exec("INSERT INTO pg_dh_orders VALUES (1, 1, 50.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_dh_orders VALUES (2, 1, 75.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_dh_orders VALUES (3, 1, 30.00, 'cancelled')");
        $this->pdo->exec("INSERT INTO pg_dh_orders VALUES (4, 2, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_dh_orders VALUES (5, 3, 10.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_dh_orders VALUES (6, 3, 15.00, 'completed')");
    }

    /**
     * DELETE customers who have total orders below threshold.
     */
    public function testDeleteWithHavingInSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_dh_customers
             WHERE id IN (
                 SELECT customer_id FROM pg_dh_orders
                 WHERE status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) < 50
             )"
        );

        $rows = $this->ztdQuery("SELECT name FROM pg_dh_customers ORDER BY name");
        $names = array_column($rows, 'name');
        $this->assertNotContains('Carol', $names);
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Dave', $names);
    }

    /**
     * DELETE with NOT EXISTS and GROUP BY HAVING.
     */
    public function testDeleteWithNotExistsHaving(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_dh_customers
             WHERE NOT EXISTS (
                 SELECT 1 FROM pg_dh_orders
                 WHERE customer_id = pg_dh_customers.id
                   AND status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) > 100
             )"
        );

        $rows = $this->ztdQuery("SELECT name FROM pg_dh_customers ORDER BY name");
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertNotContains('Carol', $names);
        $this->assertNotContains('Dave', $names);
    }

    /**
     * DELETE with HAVING and prepared parameter.
     */
    public function testDeleteWithHavingPrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM pg_dh_customers
             WHERE id IN (
                 SELECT customer_id FROM pg_dh_orders
                 WHERE status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) < $1
             )"
        );
        $stmt->execute([50]);

        $rows = $this->ztdQuery("SELECT name FROM pg_dh_customers ORDER BY name");
        $names = array_column($rows, 'name');
        $this->assertNotContains('Carol', $names);
        $this->assertContains('Alice', $names);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $c = $this->pdo->query("SELECT COUNT(*) FROM pg_dh_customers")->fetchColumn();
        $this->assertSame(0, (int) $c);
    }
}
