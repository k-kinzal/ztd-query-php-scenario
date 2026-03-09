<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DISTINCT inside aggregate functions through MySQLi CTE shadow store.
 *
 * @spec SPEC-3.3
 */
class DistinctAggregateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_da_reps (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                region VARCHAR(20) NOT NULL
            )',
            'CREATE TABLE mi_da_sales (
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
        return ['mi_da_sales', 'mi_da_reps'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_da_reps VALUES (1, 'Alice', 'East')");
        $this->mysqli->query("INSERT INTO mi_da_reps VALUES (2, 'Bob', 'East')");
        $this->mysqli->query("INSERT INTO mi_da_reps VALUES (3, 'Carol', 'West')");

        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (1, 1, 'Widget', 100.00, '2025-01-10')");
        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (2, 1, 'Widget', 100.00, '2025-01-11')");
        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (3, 1, 'Gadget', 200.00, '2025-01-12')");
        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (4, 1, 'Gadget', 200.00, '2025-01-13')");

        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (5, 2, 'Widget', 100.00, '2025-01-10')");
        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (6, 2, 'Gadget', 200.00, '2025-01-11')");
        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (7, 2, 'Gizmo', 150.00, '2025-01-12')");

        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (8, 3, 'Widget', 100.00, '2025-01-10')");
        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (9, 3, 'Widget', 100.00, '2025-01-11')");
        $this->mysqli->query("INSERT INTO mi_da_sales VALUES (10, 3, 'Widget', 100.00, '2025-01-12')");
    }

    public function testSumDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, SUM(DISTINCT s.amount) AS distinct_total
             FROM mi_da_sales s
             JOIN mi_da_reps r ON r.id = s.rep_id
             GROUP BY r.name ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(300.00, (float) $rows[0]['distinct_total']);
        $this->assertEquals(450.00, (float) $rows[1]['distinct_total']);
        $this->assertEquals(100.00, (float) $rows[2]['distinct_total']);
    }

    public function testSumDistinctVsSum(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name,
                    SUM(s.amount) AS total,
                    SUM(DISTINCT s.amount) AS distinct_total
             FROM mi_da_sales s
             JOIN mi_da_reps r ON r.id = s.rep_id
             GROUP BY r.name ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(600.00, (float) $rows[0]['total']);
        $this->assertEquals(300.00, (float) $rows[0]['distinct_total']);
        $this->assertEquals(300.00, (float) $rows[2]['total']);
        $this->assertEquals(100.00, (float) $rows[2]['distinct_total']);
    }

    public function testGroupConcatDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, GROUP_CONCAT(DISTINCT s.product ORDER BY s.product) AS products
             FROM mi_da_sales s
             JOIN mi_da_reps r ON r.id = s.rep_id
             GROUP BY r.name ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gadget,Widget', $rows[0]['products']);
        $this->assertSame('Gadget,Gizmo,Widget', $rows[1]['products']);
        $this->assertSame('Widget', $rows[2]['products']);
    }

    public function testCountDistinctWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, COUNT(DISTINCT s.product) AS unique_products
             FROM mi_da_sales s
             JOIN mi_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             HAVING COUNT(DISTINCT s.product) >= 2
             ORDER BY r.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $raw = new \mysqli(
            \Tests\Support\MySQLContainer::getHost(),
            'root', 'root', 'test',
            \Tests\Support\MySQLContainer::getPort(),
        );
        $result = $raw->query("SELECT COUNT(*) AS cnt FROM mi_da_sales");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
        $raw->close();
    }
}
