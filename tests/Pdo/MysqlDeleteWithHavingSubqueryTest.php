<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE operations where the target rows are determined by
 * subqueries involving GROUP BY and HAVING.
 *
 * Known issue: HAVING with prepared params may return empty on some platforms.
 * This tests whether DELETE with HAVING-filtered subqueries works in the
 * CTE shadow store.
 *
 * @spec SPEC-4.3
 */
class MysqlDeleteWithHavingSubqueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_dh_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            )',
            'CREATE TABLE my_dh_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                tier VARCHAR(10) NOT NULL DEFAULT "standard"
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_dh_orders', 'my_dh_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_dh_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO my_dh_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO my_dh_customers VALUES (3, 'Carol', 'standard')");
        $this->pdo->exec("INSERT INTO my_dh_customers VALUES (4, 'Dave', 'premium')");

        $this->pdo->exec("INSERT INTO my_dh_orders VALUES (1, 1, 50.00, 'completed')");
        $this->pdo->exec("INSERT INTO my_dh_orders VALUES (2, 1, 75.00, 'completed')");
        $this->pdo->exec("INSERT INTO my_dh_orders VALUES (3, 1, 30.00, 'cancelled')");
        $this->pdo->exec("INSERT INTO my_dh_orders VALUES (4, 2, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO my_dh_orders VALUES (5, 3, 10.00, 'completed')");
        $this->pdo->exec("INSERT INTO my_dh_orders VALUES (6, 3, 15.00, 'completed')");
    }

    /**
     * DELETE customers who have total orders below threshold.
     */
    public function testDeleteWithHavingInSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM my_dh_customers
             WHERE id IN (
                 SELECT customer_id FROM my_dh_orders
                 WHERE status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) < 50
             )"
        );

        $rows = $this->ztdQuery("SELECT name FROM my_dh_customers ORDER BY name");
        $names = array_column($rows, 'name');
        // Carol: SUM=25 (10+15), deleted
        // Alice: SUM=125 (50+75), kept
        // Bob: SUM=200, kept
        // Dave: no orders, kept
        $this->assertNotContains('Carol', $names, 'Carol should be deleted (total < 50)');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Dave', $names);
    }

    /**
     * DELETE orders for customers with too many cancellations.
     * Self-referencing DELETE: same table in DELETE target and IN subquery.
     *
     * BUG: CTE rewriter incorrectly deletes all rows instead of only those
     * matching the HAVING filter. Same pattern as upstream Issue #11 (UPDATE variant).
     */
    public function testDeleteOrdersWithHavingCount(): void
    {
        $this->pdo->exec(
            "DELETE FROM my_dh_orders
             WHERE customer_id IN (
                 SELECT customer_id FROM my_dh_orders
                 WHERE status = 'cancelled'
                 GROUP BY customer_id
                 HAVING COUNT(*) >= 1
             )"
        );

        $rows = $this->ztdQuery("SELECT id FROM my_dh_orders ORDER BY id");
        $ids = array_column($rows, 'id');
        // Alice (customer 1) has 1 cancellation => all her orders deleted
        $this->assertNotContains('1', $ids);
        $this->assertNotContains('2', $ids);
        $this->assertNotContains('3', $ids);
        // Bob and Carol's orders remain
        $this->assertContains('4', $ids);
        $this->assertContains('5', $ids);
    }

    /**
     * DELETE with HAVING and prepared parameter.
     */
    public function testDeleteWithHavingPrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM my_dh_customers
             WHERE id IN (
                 SELECT customer_id FROM my_dh_orders
                 WHERE status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) < ?
             )"
        );
        $stmt->execute([50]);

        $rows = $this->ztdQuery("SELECT name FROM my_dh_customers ORDER BY name");
        $names = array_column($rows, 'name');
        $this->assertNotContains('Carol', $names);
        $this->assertContains('Alice', $names);
    }

    /**
     * DELETE with NOT EXISTS and GROUP BY HAVING in correlated subquery.
     */
    public function testDeleteWithNotExistsHaving(): void
    {
        // Delete customers who have no completed orders totalling > 100
        $this->pdo->exec(
            "DELETE FROM my_dh_customers
             WHERE NOT EXISTS (
                 SELECT 1 FROM my_dh_orders
                 WHERE customer_id = my_dh_customers.id
                   AND status = 'completed'
                 GROUP BY customer_id
                 HAVING SUM(amount) > 100
             )"
        );

        $rows = $this->ztdQuery("SELECT name FROM my_dh_customers ORDER BY name");
        $names = array_column($rows, 'name');
        // Alice: completed sum=125 > 100 => KEPT
        // Bob: completed sum=200 > 100 => KEPT
        // Carol: completed sum=25 < 100 => DELETED
        // Dave: no orders => DELETED
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertNotContains('Carol', $names);
        $this->assertNotContains('Dave', $names);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $c = $this->pdo->query("SELECT COUNT(*) FROM my_dh_customers")->fetchColumn();
        $o = $this->pdo->query("SELECT COUNT(*) FROM my_dh_orders")->fetchColumn();
        $this->assertSame(0, (int) $c);
        $this->assertSame(0, (int) $o);
    }
}
