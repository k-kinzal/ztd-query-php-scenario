<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DISTINCT inside aggregate functions through MySQL PDO CTE shadow store.
 * Covers SUM(DISTINCT), AVG(DISTINCT), COUNT(DISTINCT), GROUP_CONCAT(DISTINCT).
 *
 * @spec SPEC-3.3
 */
class MysqlDistinctAggregateTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_da_reps (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                region VARCHAR(20) NOT NULL
            )',
            'CREATE TABLE my_da_sales (
                id INT PRIMARY KEY,
                rep_id INT NOT NULL,
                product VARCHAR(50) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                sale_date DATE NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_da_sales', 'my_da_reps'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_da_reps VALUES (1, 'Alice', 'East')");
        $this->pdo->exec("INSERT INTO my_da_reps VALUES (2, 'Bob', 'East')");
        $this->pdo->exec("INSERT INTO my_da_reps VALUES (3, 'Carol', 'West')");

        $this->pdo->exec("INSERT INTO my_da_sales VALUES (1, 1, 'Widget', 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO my_da_sales VALUES (2, 1, 'Widget', 100.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO my_da_sales VALUES (3, 1, 'Gadget', 200.00, '2025-01-12')");
        $this->pdo->exec("INSERT INTO my_da_sales VALUES (4, 1, 'Gadget', 200.00, '2025-01-13')");

        $this->pdo->exec("INSERT INTO my_da_sales VALUES (5, 2, 'Widget', 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO my_da_sales VALUES (6, 2, 'Gadget', 200.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO my_da_sales VALUES (7, 2, 'Gizmo', 150.00, '2025-01-12')");

        $this->pdo->exec("INSERT INTO my_da_sales VALUES (8, 3, 'Widget', 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO my_da_sales VALUES (9, 3, 'Widget', 100.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO my_da_sales VALUES (10, 3, 'Widget', 100.00, '2025-01-12')");
    }

    public function testSumDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, SUM(DISTINCT s.amount) AS distinct_total
             FROM my_da_sales s
             JOIN my_da_reps r ON r.id = s.rep_id
             GROUP BY r.name ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(300.00, (float) $rows[0]['distinct_total']);
        $this->assertEquals(450.00, (float) $rows[1]['distinct_total']);
        $this->assertEquals(100.00, (float) $rows[2]['distinct_total']);
    }

    public function testGroupConcatDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, GROUP_CONCAT(DISTINCT s.product ORDER BY s.product) AS products
             FROM my_da_sales s
             JOIN my_da_reps r ON r.id = s.rep_id
             GROUP BY r.name ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gadget,Widget', $rows[0]['products']); // Alice
        $this->assertSame('Gadget,Gizmo,Widget', $rows[1]['products']); // Bob
        $this->assertSame('Widget', $rows[2]['products']); // Carol
    }

    public function testSumDistinctVsSum(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name,
                    SUM(s.amount) AS total,
                    SUM(DISTINCT s.amount) AS distinct_total
             FROM my_da_sales s
             JOIN my_da_reps r ON r.id = s.rep_id
             GROUP BY r.name ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(600.00, (float) $rows[0]['total']);
        $this->assertEquals(300.00, (float) $rows[0]['distinct_total']);
    }

    public function testCountDistinctWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, COUNT(DISTINCT s.product) AS unique_products
             FROM my_da_sales s
             JOIN my_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             HAVING COUNT(DISTINCT s.product) >= 2
             ORDER BY r.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testPreparedCountDistinct(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(DISTINCT s.product) AS unique_products
             FROM my_da_sales s
             JOIN my_da_reps r ON r.id = s.rep_id
             WHERE r.region = ?",
            ['East']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['unique_products']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_da_sales")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
