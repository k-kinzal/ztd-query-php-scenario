<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE operations where the target rows are determined by
 * subqueries involving GROUP BY and HAVING through SQLite CTE rewriter.
 *
 * @spec SPEC-4.3
 */
class SqliteDeleteWithHavingSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dh_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL
            )',
            'CREATE TABLE sl_dh_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT \'standard\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dh_orders', 'sl_dh_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dh_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO sl_dh_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO sl_dh_customers VALUES (3, 'Carol', 'standard')");
        $this->pdo->exec("INSERT INTO sl_dh_customers VALUES (4, 'Dave', 'premium')");

        $this->pdo->exec("INSERT INTO sl_dh_orders VALUES (1, 1, 50.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_dh_orders VALUES (2, 1, 75.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_dh_orders VALUES (3, 1, 30.00, 'cancelled')");
        $this->pdo->exec("INSERT INTO sl_dh_orders VALUES (4, 2, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_dh_orders VALUES (5, 3, 10.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_dh_orders VALUES (6, 3, 15.00, 'completed')");
    }

    /**
     * DELETE customers who have total orders below threshold.
     */
    public function testDeleteWithHavingInSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_dh_customers
             WHERE id IN (
                 SELECT customer_id FROM sl_dh_orders
                 WHERE status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) < 50
             )"
        );

        $rows = $this->ztdQuery("SELECT name FROM sl_dh_customers ORDER BY name");
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
            "DELETE FROM sl_dh_customers
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_dh_orders
                 WHERE customer_id = sl_dh_customers.id
                   AND status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) > 100
             )"
        );

        $rows = $this->ztdQuery("SELECT name FROM sl_dh_customers ORDER BY name");
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
            "DELETE FROM sl_dh_customers
             WHERE id IN (
                 SELECT customer_id FROM sl_dh_orders
                 WHERE status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) < ?
             )"
        );
        $stmt->execute([50]);

        $rows = $this->ztdQuery("SELECT name FROM sl_dh_customers ORDER BY name");
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
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dh_customers');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
