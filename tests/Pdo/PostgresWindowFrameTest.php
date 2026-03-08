<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests window functions with FRAME clauses (ROWS/RANGE BETWEEN) on PostgreSQL PDO.
 * @spec pending
 */
class PostgresWindowFrameTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_wf_sales (id INT PRIMARY KEY, month VARCHAR(10), amount NUMERIC(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['pg_wf_sales'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_wf_sales (id, month, amount) VALUES (1, '2024-01', 100)");
        $this->pdo->exec("INSERT INTO pg_wf_sales (id, month, amount) VALUES (2, '2024-02', 200)");
        $this->pdo->exec("INSERT INTO pg_wf_sales (id, month, amount) VALUES (3, '2024-03', 150)");
        $this->pdo->exec("INSERT INTO pg_wf_sales (id, month, amount) VALUES (4, '2024-04', 300)");
        $this->pdo->exec("INSERT INTO pg_wf_sales (id, month, amount) VALUES (5, '2024-05', 250)");
    }

    public function testWindowFunctionRowsBetween(): void
    {
        $stmt = $this->pdo->query("
            SELECT month, amount,
                   AVG(amount) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_avg
            FROM pg_wf_sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(150.0, (float) $rows[0]['rolling_avg'], 0.01);
    }

    public function testWindowFunctionCumulativeSum(): void
    {
        $stmt = $this->pdo->query("
            SELECT month, amount,
                   SUM(amount) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative
            FROM pg_wf_sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(300.0, (float) $rows[1]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(1000.0, (float) $rows[4]['cumulative'], 0.01);
    }

    public function testWindowFunctionLagLead(): void
    {
        $stmt = $this->pdo->query("
            SELECT month, amount,
                   LAG(amount, 1) OVER (ORDER BY month) AS prev_amount,
                   LEAD(amount, 1) OVER (ORDER BY month) AS next_amount
            FROM pg_wf_sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertNull($rows[0]['prev_amount']);
        $this->assertSame(200.0, (float) $rows[0]['next_amount']);
    }

    public function testWindowFunctionAfterMutations(): void
    {
        $this->pdo->exec("UPDATE pg_wf_sales SET amount = 500 WHERE id = 2");
        $this->pdo->exec("DELETE FROM pg_wf_sales WHERE id = 3");

        $stmt = $this->pdo->query("
            SELECT month, amount,
                   SUM(amount) OVER (ORDER BY month) AS cumulative
            FROM pg_wf_sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(600.0, (float) $rows[1]['cumulative'], 0.01);
    }
}
