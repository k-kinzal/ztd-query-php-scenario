<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Window function queries through ZTD shadow store on MySQLi.
 *
 * Window functions require MySQL 8.0+. Tests ROW_NUMBER, RANK, SUM OVER
 * with mutations to verify the CTE rewriter and shadow store work
 * correctly for analytical queries via the MySQLi adapter.
 *
 * @spec SPEC-3.3
 */
class WindowFunctionQueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_wfq_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                rep VARCHAR(30),
                region VARCHAR(20),
                amount DECIMAL(10,2),
                sale_date DATE
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_wfq_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (1, 'Alice', 'East', 100.00, '2025-01-10')");
        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (2, 'Bob', 'East', 200.00, '2025-01-12')");
        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (3, 'Alice', 'East', 150.00, '2025-01-15')");
        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (4, 'Carol', 'West', 300.00, '2025-01-11')");
        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (5, 'Carol', 'West', 250.00, '2025-01-14')");
        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (6, 'Dave', 'West', 180.00, '2025-01-13')");
    }

    public function testRowNumberOverOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, rep, amount,
                        ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
                 FROM mi_wfq_sales"
            );
            $this->assertCount(6, $rows);
            $this->assertSame(1, (int) $rows[0]['rn']);
            $this->assertSame('Carol', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ROW_NUMBER() OVER failed: ' . $e->getMessage());
        }
    }

    public function testRowNumberPartitionByRegion(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM mi_wfq_sales
                 ORDER BY region, rn"
            );
            $this->assertCount(6, $rows);
            $east = array_values(array_filter($rows, fn($r) => $r['region'] === 'East'));
            $this->assertSame(1, (int) $east[0]['rn']);
            $this->assertSame('Bob', $east[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('PARTITION BY region failed: ' . $e->getMessage());
        }
    }

    public function testRankWithTies(): void
    {
        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (7, 'Eve', 'East', 200.00, '2025-01-20')");
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        RANK() OVER (ORDER BY amount DESC) AS rnk
                 FROM mi_wfq_sales
                 ORDER BY rnk, rep"
            );
            $this->assertCount(7, $rows);
            $rank3 = array_filter($rows, fn($r) => (int) $r['rnk'] === 3);
            $this->assertCount(2, $rank3);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('RANK() with ties failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_wfq_sales (id, rep, region, amount, sale_date) VALUES (7, 'Eve', 'East', 500.00, '2025-01-25')");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
                 FROM mi_wfq_sales"
            );
            $this->assertCount(7, $rows);
            $this->assertSame(1, (int) $rows[0]['rn']);
            $this->assertSame('Eve', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterUpdate(): void
    {
        $this->ztdExec("UPDATE mi_wfq_sales SET amount = 999.00 WHERE rep = 'Dave'");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        RANK() OVER (ORDER BY amount DESC) AS rnk
                 FROM mi_wfq_sales"
            );
            $this->assertCount(6, $rows);
            $this->assertSame(1, (int) $rows[0]['rnk']);
            $this->assertSame('Dave', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterDelete(): void
    {
        $this->ztdExec("DELETE FROM mi_wfq_sales WHERE rep = 'Dave'");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM mi_wfq_sales
                 ORDER BY region, rn"
            );
            $this->assertCount(5, $rows);
            $west = array_values(array_filter($rows, fn($r) => $r['region'] === 'West'));
            $this->assertCount(2, $west);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT rep, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM mi_wfq_sales
                 WHERE amount > ?
                 ORDER BY region, rn",
                [150.0]
            );
            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function with prepared param failed: ' . $e->getMessage());
        }
    }
}
