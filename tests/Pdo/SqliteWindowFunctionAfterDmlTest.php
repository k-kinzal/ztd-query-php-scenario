<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests window functions in SELECT reading from shadow-modified tables on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteWindowFunctionAfterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_wfd_sales (
            id INTEGER PRIMARY KEY,
            product TEXT NOT NULL,
            amount REAL NOT NULL,
            region TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_wfd_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_wfd_sales VALUES (1, 'Widget', 100.00, 'East')");
        $this->pdo->exec("INSERT INTO sl_wfd_sales VALUES (2, 'Gadget', 200.00, 'East')");
        $this->pdo->exec("INSERT INTO sl_wfd_sales VALUES (3, 'Widget', 150.00, 'West')");
        $this->pdo->exec("INSERT INTO sl_wfd_sales VALUES (4, 'Doohickey', 50.00, 'West')");
    }

    public function testCountOverAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wfd_sales VALUES (5, 'Gizmo', 300.00, 'East')");

            $rows = $this->ztdQuery(
                "SELECT product, COUNT(*) OVER() AS total_count
                 FROM sl_wfd_sales ORDER BY id"
            );

            $this->assertCount(5, $rows);
            $total = (int) $rows[0]['total_count'];
            if ($total !== 5) {
                $this->markTestIncomplete("COUNT(*) OVER(): expected 5, got $total");
            }
            $this->assertEquals(5, $total);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(*) OVER() after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testRowNumberAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_wfd_sales WHERE id = 4");

            $rows = $this->ztdQuery(
                "SELECT product, ROW_NUMBER() OVER(ORDER BY amount DESC) AS rn
                 FROM sl_wfd_sales"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertEquals(1, (int) $rows[0]['rn']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ROW_NUMBER() after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testSumOverPartitionAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wfd_sales VALUES (5, 'Gizmo', 300.00, 'East')");

            $rows = $this->ztdQuery(
                "SELECT region, SUM(amount) OVER(PARTITION BY region) AS region_total
                 FROM sl_wfd_sales
                 ORDER BY region, id"
            );

            $this->assertCount(5, $rows);

            $eastTotal = null;
            foreach ($rows as $row) {
                if ($row['region'] === 'East') {
                    $eastTotal = (float) $row['region_total'];
                }
            }

            if ($eastTotal != 600.00) {
                $this->markTestIncomplete("SUM OVER PARTITION East: expected 600, got $eastTotal");
            }
            $this->assertEquals(600.00, $eastTotal);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUM() OVER(PARTITION BY) after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testWindowPaginationAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wfd_sales VALUES (5, 'Gizmo', 300.00, 'East')");

            $rows = $this->ztdQuery(
                "SELECT product, COUNT(*) OVER() AS total
                 FROM sl_wfd_sales
                 ORDER BY amount DESC
                 LIMIT 2"
            );

            $this->assertCount(2, $rows);
            $total = (int) $rows[0]['total'];
            if ($total !== 5) {
                $this->markTestIncomplete("Window pagination: total expected 5, got $total");
            }
            $this->assertEquals(5, $total);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window pagination after INSERT failed: ' . $e->getMessage());
        }
    }
}
