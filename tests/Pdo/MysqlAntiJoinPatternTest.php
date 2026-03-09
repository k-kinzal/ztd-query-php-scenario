<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests anti-join patterns through MySQL PDO CTE shadow store.
 *
 * @spec SPEC-3.3
 */
class MysqlAntiJoinPatternTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_aj_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL
            )',
            'CREATE TABLE my_aj_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                total DECIMAL(10,2) NOT NULL,
                order_date DATE NOT NULL
            )',
            'CREATE TABLE my_aj_returns (
                id INT PRIMARY KEY,
                order_id INT NOT NULL,
                reason VARCHAR(100) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_aj_returns', 'my_aj_orders', 'my_aj_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_aj_customers VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO my_aj_customers VALUES (2, 'Bob', 'bob@example.com')");
        $this->pdo->exec("INSERT INTO my_aj_customers VALUES (3, 'Carol', 'carol@example.com')");
        $this->pdo->exec("INSERT INTO my_aj_customers VALUES (4, 'Dave', 'dave@example.com')");
        $this->pdo->exec("INSERT INTO my_aj_customers VALUES (5, 'Eve', 'eve@example.com')");

        $this->pdo->exec("INSERT INTO my_aj_orders VALUES (1, 1, 150.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO my_aj_orders VALUES (2, 1, 200.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO my_aj_orders VALUES (3, 2, 75.00, '2025-01-12')");

        $this->pdo->exec("INSERT INTO my_aj_returns VALUES (1, 1, 'Defective')");
    }

    public function testAntiJoinLeftJoinWhereNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM my_aj_customers c
             LEFT JOIN my_aj_orders o ON o.customer_id = c.id
             WHERE o.id IS NULL
             ORDER BY c.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
    }

    public function testAntiJoinNotExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM my_aj_customers c
             WHERE NOT EXISTS (
                 SELECT 1 FROM my_aj_orders o WHERE o.customer_id = c.id
             )
             ORDER BY c.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
    }

    public function testAntiJoinNotIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM my_aj_customers c
             WHERE c.id NOT IN (SELECT customer_id FROM my_aj_orders)
             ORDER BY c.name"
        );

        $this->assertCount(3, $rows);
    }

    public function testChainedAntiJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.id AS order_id, c.name
             FROM my_aj_orders o
             JOIN my_aj_customers c ON c.id = o.customer_id
             LEFT JOIN my_aj_returns r ON r.order_id = o.id
             WHERE r.id IS NULL
             ORDER BY o.id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(2, (int) $rows[0]['order_id']);
        $this->assertEquals(3, (int) $rows[1]['order_id']);
    }

    public function testDoubleNotExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM my_aj_customers c
             WHERE EXISTS (
                 SELECT 1 FROM my_aj_orders o WHERE o.customer_id = c.id
             )
             AND NOT EXISTS (
                 SELECT 1 FROM my_aj_orders o
                 JOIN my_aj_returns r ON r.order_id = o.id
                 WHERE o.customer_id = c.id
             )
             ORDER BY c.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPreparedNotExists(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.name
             FROM my_aj_customers c
             WHERE NOT EXISTS (
                 SELECT 1 FROM my_aj_orders o
                 WHERE o.customer_id = c.id AND o.total > ?
             )
             ORDER BY c.name",
            [100.00]
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_aj_customers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
