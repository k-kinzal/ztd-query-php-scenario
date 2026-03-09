<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests window functions (ROW_NUMBER, RANK, LAG, LEAD, SUM OVER) on shadow data.
 *
 * Window functions are commonly used in pagination, ranking, and analytics
 * queries. The CTE rewriter must preserve window function semantics when
 * reading from shadow data.
 *
 * Known issue: #13 — window functions in derived tables return empty.
 * This tests window functions in the main SELECT (not derived tables).
 *
 * @spec SPEC-3.3
 */
class SqliteWindowFunctionShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE wf_t (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['wf_t'];
    }

    private function seedData(): void
    {
        $this->pdo->exec("INSERT INTO wf_t (id, name, dept, salary) VALUES
            (1, 'Alice', 'Engineering', 90000),
            (2, 'Bob', 'Engineering', 85000),
            (3, 'Charlie', 'Sales', 70000),
            (4, 'Diana', 'Sales', 75000),
            (5, 'Eve', 'Engineering', 95000)");
    }

    /**
     * ROW_NUMBER() OVER (ORDER BY ...) on shadow data.
     */
    public function testRowNumberOverOrderBy(): void
    {
        $this->seedData();

        $rows = $this->ztdQuery(
            'SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM wf_t'
        );
        $this->assertCount(5, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['rn']);
    }

    /**
     * ROW_NUMBER() with PARTITION BY.
     */
    public function testRowNumberWithPartitionBy(): void
    {
        $this->seedData();

        $rows = $this->ztdQuery(
            'SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM wf_t ORDER BY dept, rn'
        );
        $this->assertCount(5, $rows);
        // Engineering: Eve(1), Alice(2), Bob(3)
        $engRows = array_filter($rows, fn($r) => $r['dept'] === 'Engineering');
        $engRows = array_values($engRows);
        $this->assertSame('Eve', $engRows[0]['name']);
        $this->assertEquals(1, (int) $engRows[0]['rn']);
    }

    /**
     * RANK() with ties.
     */
    public function testRankWithTies(): void
    {
        $this->pdo->exec("INSERT INTO wf_t (id, name, dept, salary) VALUES
            (1, 'A', 'X', 100),
            (2, 'B', 'X', 100),
            (3, 'C', 'X', 90)");

        $rows = $this->ztdQuery(
            'SELECT name, RANK() OVER (ORDER BY salary DESC) AS rnk FROM wf_t'
        );
        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['rnk']); // A or B (tied)
        $this->assertEquals(1, (int) $rows[1]['rnk']); // A or B (tied)
        $this->assertEquals(3, (int) $rows[2]['rnk']); // C (rank 3, not 2)
    }

    /**
     * SUM() OVER (ORDER BY) — running total.
     */
    public function testSumOverRunningTotal(): void
    {
        $this->pdo->exec("INSERT INTO wf_t (id, name, dept, salary) VALUES
            (1, 'A', 'X', 10),
            (2, 'B', 'X', 20),
            (3, 'C', 'X', 30)");

        $rows = $this->ztdQuery(
            'SELECT name, SUM(salary) OVER (ORDER BY id) AS running_total FROM wf_t'
        );
        $this->assertCount(3, $rows);
        $this->assertEquals(10, (int) $rows[0]['running_total']);
        $this->assertEquals(30, (int) $rows[1]['running_total']);
        $this->assertEquals(60, (int) $rows[2]['running_total']);
    }

    /**
     * LAG() function on shadow data.
     */
    public function testLagFunction(): void
    {
        $this->pdo->exec("INSERT INTO wf_t (id, name, dept, salary) VALUES
            (1, 'A', 'X', 100),
            (2, 'B', 'X', 200),
            (3, 'C', 'X', 300)");

        $rows = $this->ztdQuery(
            'SELECT name, salary, LAG(salary, 1) OVER (ORDER BY id) AS prev_salary FROM wf_t'
        );
        $this->assertCount(3, $rows);
        $this->assertNull($rows[0]['prev_salary']); // No previous
        $this->assertEquals(100, (int) $rows[1]['prev_salary']);
        $this->assertEquals(200, (int) $rows[2]['prev_salary']);
    }

    /**
     * Window function after shadow mutation.
     */
    public function testWindowFunctionAfterMutation(): void
    {
        $this->seedData();
        $this->pdo->exec("UPDATE wf_t SET salary = 200000 WHERE id = 3"); // Charlie becomes top earner

        $rows = $this->ztdQuery(
            'SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM wf_t'
        );
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['rn']);
    }

    /**
     * Window function after INSERT adds new rows to ranking.
     */
    public function testWindowFunctionAfterInsert(): void
    {
        $this->seedData();
        $this->pdo->exec("INSERT INTO wf_t (id, name, dept, salary) VALUES (6, 'Frank', 'Engineering', 200000)");

        $rows = $this->ztdQuery(
            'SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM wf_t'
        );
        $this->assertCount(6, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['rn']);
    }

    /**
     * Window function with prepared params in WHERE.
     */
    public function testWindowFunctionWithPreparedParams(): void
    {
        $this->seedData();

        $rows = $this->ztdPrepareAndExecute(
            'SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM wf_t WHERE dept = ?',
            ['Engineering']
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
    }
}
