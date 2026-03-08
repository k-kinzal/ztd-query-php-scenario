<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE with correlated subqueries on MySQL PDO.
 *
 * Cross-platform parity with SqliteDeleteWithCorrelatedSubqueryTest.
 * @spec pending
 */
class MysqlDeleteWithCorrelatedSubqueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_del_customers (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)',
            'CREATE TABLE pdo_del_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_del_orders', 'pdo_del_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_del_customers VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO pdo_del_customers VALUES (3, 'Charlie', 1)");
        $this->pdo->exec("INSERT INTO pdo_del_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pdo_del_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO pdo_del_orders VALUES (3, 3, 150.00)");
    }

    /**
     * DELETE with EXISTS correlated subquery.
     */
    public function testDeleteWithExistsSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM pdo_del_customers
            WHERE EXISTS (SELECT 1 FROM pdo_del_orders o WHERE o.customer_id = pdo_del_customers.id)
        ');

        $stmt = $this->pdo->query('SELECT name FROM pdo_del_customers ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]);
    }

    /**
     * DELETE with NOT EXISTS.
     */
    public function testDeleteWithNotExistsSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM pdo_del_customers
            WHERE NOT EXISTS (SELECT 1 FROM pdo_del_orders o WHERE o.customer_id = pdo_del_customers.id)
        ');

        $stmt = $this->pdo->query('SELECT name FROM pdo_del_customers ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * DELETE with IN subquery.
     */
    public function testDeleteWithInSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM pdo_del_orders
            WHERE customer_id IN (SELECT id FROM pdo_del_customers WHERE active = 0)
        ');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_del_orders');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('
            DELETE FROM pdo_del_customers
            WHERE EXISTS (SELECT 1 FROM pdo_del_orders o WHERE o.customer_id = pdo_del_customers.id)
        ');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_del_customers');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
