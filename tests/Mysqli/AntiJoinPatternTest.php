<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests anti-join patterns through MySQLi CTE shadow store.
 *
 * @spec SPEC-3.3
 */
class AntiJoinPatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_aj_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )',
            'CREATE TABLE mi_aj_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                total DECIMAL(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_aj_orders', 'mi_aj_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_aj_customers VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_aj_customers VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_aj_customers VALUES (3, 'Carol')");
        $this->mysqli->query("INSERT INTO mi_aj_customers VALUES (4, 'Dave')");

        $this->mysqli->query("INSERT INTO mi_aj_orders VALUES (1, 1, 150.00)");
        $this->mysqli->query("INSERT INTO mi_aj_orders VALUES (2, 2, 75.00)");
    }

    public function testAntiJoinLeftJoinWhereNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM mi_aj_customers c
             LEFT JOIN mi_aj_orders o ON o.customer_id = c.id
             WHERE o.id IS NULL
             ORDER BY c.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
    }

    public function testAntiJoinNotExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM mi_aj_customers c
             WHERE NOT EXISTS (
                 SELECT 1 FROM mi_aj_orders o WHERE o.customer_id = c.id
             )
             ORDER BY c.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
    }

    public function testAntiJoinNotIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM mi_aj_customers c
             WHERE c.id NOT IN (SELECT customer_id FROM mi_aj_orders)
             ORDER BY c.name"
        );

        $this->assertCount(2, $rows);
    }

    public function testAntiJoinAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_aj_orders VALUES (3, 3, 50.00)");

        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM mi_aj_customers c
             LEFT JOIN mi_aj_orders o ON o.customer_id = c.id
             WHERE o.id IS NULL
             ORDER BY c.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Dave', $rows[0]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $raw = new \mysqli(
            \Tests\Support\MySQLContainer::getHost(),
            'root', 'root', 'test',
            \Tests\Support\MySQLContainer::getPort(),
        );
        $result = $raw->query("SELECT COUNT(*) AS cnt FROM mi_aj_customers");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
        $raw->close();
    }
}
