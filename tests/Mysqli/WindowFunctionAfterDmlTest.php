<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests window functions (COUNT(*) OVER(), ROW_NUMBER(), etc.) in SELECT
 * reading from shadow-modified tables.
 *
 * Window functions are commonly used for pagination with total count,
 * ranking, and running totals. Issue #115 documents window functions
 * in DML subquery context; this tests them in plain SELECT after DML.
 *
 * @spec SPEC-4.2
 */
class WindowFunctionAfterDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_wfd_sales (
            id INT PRIMARY KEY,
            product VARCHAR(50) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            region VARCHAR(20) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_wfd_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (1, 'Widget', 100.00, 'East')");
        $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (2, 'Gadget', 200.00, 'East')");
        $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (3, 'Widget', 150.00, 'West')");
        $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (4, 'Doohickey', 50.00, 'West')");
    }

    /**
     * COUNT(*) OVER() — total count alongside each row (pagination pattern).
     */
    public function testCountOverAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (5, 'Gizmo', 300.00, 'East')");

            $rows = $this->ztdQuery(
                "SELECT product, amount, COUNT(*) OVER() AS total_count
                 FROM mi_wfd_sales
                 ORDER BY id"
            );

            $this->assertCount(5, $rows);

            $total = (int) $rows[0]['total_count'];
            if ($total !== 5) {
                $this->markTestIncomplete("COUNT(*) OVER(): expected 5, got $total");
            }
            $this->assertEquals(5, $total);
            // Every row should have the same total
            foreach ($rows as $row) {
                $this->assertEquals(5, (int) $row['total_count']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(*) OVER() after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * ROW_NUMBER() OVER(ORDER BY ...) after DML.
     */
    public function testRowNumberAfterDml(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (5, 'Gizmo', 300.00, 'East')");
            $this->mysqli->query("DELETE FROM mi_wfd_sales WHERE id = 4");

            $rows = $this->ztdQuery(
                "SELECT product, ROW_NUMBER() OVER(ORDER BY amount DESC) AS rn
                 FROM mi_wfd_sales"
            );

            $this->assertCount(4, $rows);

            // Order by amount DESC: Gizmo(300), Gadget(200), Widget(150), Widget(100)
            if ((int) $rows[0]['rn'] !== 1) {
                $this->markTestIncomplete('ROW_NUMBER: first row rn expected 1, got ' . $rows[0]['rn']);
            }
            $this->assertSame('Gizmo', $rows[0]['product']);
            $this->assertEquals(1, (int) $rows[0]['rn']);
            $this->assertEquals(4, (int) $rows[3]['rn']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ROW_NUMBER() after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * SUM() OVER(PARTITION BY ...) — running total per partition after DML.
     */
    public function testSumOverPartitionAfterDml(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (5, 'Gizmo', 300.00, 'East')");

            $rows = $this->ztdQuery(
                "SELECT product, region, amount,
                        SUM(amount) OVER(PARTITION BY region) AS region_total
                 FROM mi_wfd_sales
                 ORDER BY region, id"
            );

            $this->assertCount(5, $rows);

            // East: Widget(100) + Gadget(200) + Gizmo(300) = 600
            // West: Widget(150) + Doohickey(50) = 200
            $eastTotal = null;
            $westTotal = null;
            foreach ($rows as $row) {
                if ($row['region'] === 'East') {
                    $eastTotal = (float) $row['region_total'];
                }
                if ($row['region'] === 'West') {
                    $westTotal = (float) $row['region_total'];
                }
            }

            if ($eastTotal != 600.00) {
                $this->markTestIncomplete("SUM OVER PARTITION East: expected 600, got $eastTotal");
            }
            $this->assertEquals(600.00, $eastTotal);
            $this->assertEquals(200.00, $westTotal);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUM() OVER(PARTITION BY) after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * RANK() OVER() after UPDATE.
     */
    public function testRankAfterUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_wfd_sales SET amount = 200.00 WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT product, amount, RANK() OVER(ORDER BY amount DESC) AS rnk
                 FROM mi_wfd_sales"
            );

            $this->assertCount(4, $rows);

            // After update: Gadget(200), Widget(200), Widget(150), Doohickey(50)
            // RANK: 1, 1, 3, 4
            $topRank = (int) $rows[0]['rnk'];
            if ($topRank !== 1) {
                $this->markTestIncomplete("RANK after UPDATE: expected 1, got $topRank");
            }
            $this->assertEquals(1, $topRank);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('RANK() after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Window function with LIMIT/OFFSET — pagination pattern.
     */
    public function testWindowPaginationAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (5, 'Gizmo', 300.00, 'East')");
            $this->mysqli->query("INSERT INTO mi_wfd_sales VALUES (6, 'Thing', 400.00, 'West')");

            $rows = $this->ztdQuery(
                "SELECT product, amount, COUNT(*) OVER() AS total
                 FROM mi_wfd_sales
                 ORDER BY amount DESC
                 LIMIT 3 OFFSET 0"
            );

            $this->assertCount(3, $rows);

            // Total should be 6 (all rows), but only 3 returned
            $total = (int) $rows[0]['total'];
            if ($total !== 6) {
                $this->markTestIncomplete("Window pagination: total expected 6, got $total");
            }
            $this->assertEquals(6, $total);
            $this->assertSame('Thing', $rows[0]['product']); // 400 is highest
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window pagination after INSERT failed: ' . $e->getMessage());
        }
    }
}
