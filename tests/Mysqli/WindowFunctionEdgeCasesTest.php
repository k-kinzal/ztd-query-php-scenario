<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Extended window function scenarios: RANGE frames, NULL in window,
 * multiple windows with different partitions, NTILE, window after mutation.
 * @spec SPEC-10.2.23
 */
class WindowFunctionEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_wfe_metrics (
            id INT PRIMARY KEY,
            department VARCHAR(50),
            employee VARCHAR(255),
            salary INT,
            hire_date VARCHAR(20)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_wfe_metrics'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_wfe_metrics VALUES (1, 'Eng', 'Alice', 90000, '2020-01-15')");
        $this->mysqli->query("INSERT INTO mi_wfe_metrics VALUES (2, 'Eng', 'Bob', 85000, '2021-03-10')");
        $this->mysqli->query("INSERT INTO mi_wfe_metrics VALUES (3, 'Eng', 'Charlie', 95000, '2019-06-01')");
        $this->mysqli->query("INSERT INTO mi_wfe_metrics VALUES (4, 'Sales', 'Diana', 70000, '2022-01-05')");
        $this->mysqli->query("INSERT INTO mi_wfe_metrics VALUES (5, 'Sales', 'Eve', 75000, '2021-08-20')");
        $this->mysqli->query("INSERT INTO mi_wfe_metrics VALUES (6, 'Sales', 'Frank', NULL, '2023-01-10')");
    }

    public function testRowNumberPartitionBy(): void
    {
        $rows = $this->ztdQuery("
            SELECT employee, department, salary,
                   ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank
            FROM mi_wfe_metrics
            WHERE salary IS NOT NULL
            ORDER BY department, dept_rank
        ");
        $this->assertCount(5, $rows);
        // Eng: Charlie(95k)=1, Alice(90k)=2, Bob(85k)=3
        $this->assertSame('Charlie', $rows[0]['employee']);
        $this->assertSame(1, (int) $rows[0]['dept_rank']);
    }

    public function testMultipleWindowsDifferentPartitions(): void
    {
        $rows = $this->ztdQuery("
            SELECT employee, department, salary,
                   RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank,
                   RANK() OVER (ORDER BY salary DESC) AS overall_rank
            FROM mi_wfe_metrics
            WHERE salary IS NOT NULL
            ORDER BY overall_rank
        ");
        $this->assertCount(5, $rows);
        $this->assertSame('Charlie', $rows[0]['employee']);
        $this->assertSame(1, (int) $rows[0]['dept_rank']);
        $this->assertSame(1, (int) $rows[0]['overall_rank']);
    }

    public function testNtileFunction(): void
    {
        $rows = $this->ztdQuery("
            SELECT employee, salary,
                   NTILE(3) OVER (ORDER BY salary DESC) AS quartile
            FROM mi_wfe_metrics
            WHERE salary IS NOT NULL
            ORDER BY salary DESC
        ");
        $this->assertCount(5, $rows);
        $this->assertSame(1, (int) $rows[0]['quartile']); // Top tier
        $this->assertSame(3, (int) $rows[4]['quartile']); // Bottom tier
    }

    public function testLagLeadWithNulls(): void
    {
        $rows = $this->ztdQuery("
            SELECT employee, salary,
                   LAG(salary) OVER (ORDER BY hire_date) AS prev_salary,
                   LEAD(salary) OVER (ORDER BY hire_date) AS next_salary
            FROM mi_wfe_metrics
            ORDER BY hire_date
        ");
        $this->assertCount(6, $rows);
        // First row has no LAG -> NULL
        $this->assertNull($rows[0]['prev_salary']);
        // Last row has no LEAD -> NULL
        $this->assertNull($rows[5]['next_salary']);
    }

    public function testSumOverWithNullValues(): void
    {
        // Running sum should treat NULL as 0 in SUM
        $rows = $this->ztdQuery("
            SELECT employee, salary,
                   SUM(salary) OVER (ORDER BY hire_date ROWS UNBOUNDED PRECEDING) AS running_total
            FROM mi_wfe_metrics
            ORDER BY hire_date
        ");
        $this->assertCount(6, $rows);
        // SUM skips NULLs -- running total should not decrease at Frank's row
        $lastTotal = null;
        foreach ($rows as $row) {
            if ($row['salary'] !== null) {
                $this->assertNotNull($row['running_total']);
            }
            if ($lastTotal !== null && $row['running_total'] !== null) {
                $this->assertGreaterThanOrEqual((int) $lastTotal, (int) $row['running_total']);
            }
            $lastTotal = $row['running_total'];
        }
    }

    public function testRowsBetweenFrame(): void
    {
        $rows = $this->ztdQuery("
            SELECT employee, salary,
                   AVG(salary) OVER (
                       ORDER BY salary
                       ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                   ) AS moving_avg
            FROM mi_wfe_metrics
            WHERE salary IS NOT NULL
            ORDER BY salary
        ");
        $this->assertCount(5, $rows);
        // Each row's moving_avg is avg of itself and up to 1 neighbor on each side
        $this->assertNotNull($rows[0]['moving_avg']);
    }

    public function testFirstValueLastValue(): void
    {
        $rows = $this->ztdQuery("
            SELECT employee, department, salary,
                   FIRST_VALUE(employee) OVER (PARTITION BY department ORDER BY salary DESC) AS top_earner,
                   LAST_VALUE(employee) OVER (
                       PARTITION BY department ORDER BY salary DESC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                   ) AS lowest_earner
            FROM mi_wfe_metrics
            WHERE salary IS NOT NULL
            ORDER BY department, salary DESC
        ");
        // Eng: top=Charlie(95k), lowest=Bob(85k)
        $this->assertSame('Charlie', $rows[0]['top_earner']);
        $this->assertSame('Bob', $rows[2]['lowest_earner']);
    }

    public function testWindowAfterMutation(): void
    {
        // Give Eve a raise
        $this->mysqli->query("UPDATE mi_wfe_metrics SET salary = 80000 WHERE employee = 'Eve'");

        $rows = $this->ztdQuery("
            SELECT employee, salary,
                   RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank
            FROM mi_wfe_metrics
            WHERE department = 'Sales' AND salary IS NOT NULL
            ORDER BY dept_rank
        ");
        $this->assertSame('Eve', $rows[0]['employee']);
        $this->assertSame(80000, (int) $rows[0]['salary']);
        $this->assertSame(1, (int) $rows[0]['dept_rank']);
    }

    public function testDenseRankWithTies(): void
    {
        // Insert a tie
        $this->mysqli->query("INSERT INTO mi_wfe_metrics VALUES (7, 'Eng', 'Grace', 90000, '2023-05-01')");

        $rows = $this->ztdQuery("
            SELECT employee, salary,
                   RANK() OVER (ORDER BY salary DESC) AS rank_val,
                   DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank_val
            FROM mi_wfe_metrics
            WHERE salary IS NOT NULL
            ORDER BY salary DESC, employee
        ");
        // Charlie=95k, Alice=90k=Grace, Bob=85k, Eve=75k, Diana=70k
        // RANK: 1, 2, 2, 4, 5, 6
        // DENSE_RANK: 1, 2, 2, 3, 4, 5
        $this->assertSame(1, (int) $rows[0]['rank_val']);
        $aliceAndGrace = array_filter($rows, fn($r) => (int) $r['salary'] === 90000);
        foreach ($aliceAndGrace as $r) {
            $this->assertSame(2, (int) $r['dense_rank_val']);
        }
    }

    public function testPreparedWithWindowFunction(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT employee, salary,
                    RANK() OVER (ORDER BY salary DESC) AS rnk
             FROM mi_wfe_metrics
             WHERE department = ? AND salary IS NOT NULL
             ORDER BY rnk",
            ['Eng']
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['employee']);
    }
}
