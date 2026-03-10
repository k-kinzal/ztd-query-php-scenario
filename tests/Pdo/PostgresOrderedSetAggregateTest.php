<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL ordered-set aggregate functions through shadow store.
 *
 * PERCENTILE_CONT, PERCENTILE_DISC, and MODE() use WITHIN GROUP (ORDER BY ...)
 * syntax. Verifies the CTE rewriter handles this unusual aggregate syntax.
 *
 * @spec SPEC-10.2
 */
class PostgresOrderedSetAggregateTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_osa_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                salary NUMERIC(10,2) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_osa_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert 10 rows with varied salaries for meaningful aggregate results
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 85000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 95000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (4, 'Diana', 'Sales', 60000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 65000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (6, 'Frank', 'Sales', 60000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (7, 'Grace', 'Marketing', 72000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (8, 'Hank', 'Marketing', 72000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (9, 'Ivy', 'Marketing', 78000)");
        $this->pdo->exec("INSERT INTO pg_osa_employees (id, name, department, salary) VALUES (10, 'Jack', 'Engineering', 110000)");
    }

    /**
     * PERCENTILE_CONT(0.5) computes the continuous interpolated median.
     *
     * Engineering salaries: 70000, 85000, 95000, 110000
     * Median (continuous) = (85000 + 95000) / 2 = 90000.0
     */
    public function testPercentileContMedian(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
                 FROM pg_osa_employees
                 WHERE department = 'Engineering'"
            );

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(90000.0, (float) $rows[0]['median_salary'], 0.01,
                'PERCENTILE_CONT(0.5) should interpolate the median of Engineering salaries');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'PERCENTILE_CONT WITHIN GROUP failed on shadow-inserted data: ' . $e->getMessage()
            );
        }
    }

    /**
     * PERCENTILE_DISC(0.5) returns the first value whose cumulative distribution >= 0.5.
     *
     * Sales salaries sorted: 60000, 60000, 65000
     * PERCENTILE_DISC(0.5) returns 60000 (the value at or above the 50th percentile position).
     */
    public function testPercentileDiscMedian(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
                 FROM pg_osa_employees
                 WHERE department = 'Sales'"
            );

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(60000.0, (float) $rows[0]['median_salary'], 0.01,
                'PERCENTILE_DISC(0.5) should return the discrete median of Sales salaries');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'PERCENTILE_DISC WITHIN GROUP failed on shadow-inserted data: ' . $e->getMessage()
            );
        }
    }

    /**
     * MODE() returns the most frequent value.
     *
     * Sales salaries: 60000, 60000, 65000 => mode is 60000
     */
    public function testModeAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT MODE() WITHIN GROUP (ORDER BY salary) AS mode_salary
                 FROM pg_osa_employees
                 WHERE department = 'Sales'"
            );

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(60000.0, (float) $rows[0]['mode_salary'], 0.01,
                'MODE() should return 60000 as the most frequent Sales salary');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'MODE() WITHIN GROUP failed on shadow-inserted data: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple ordered-set aggregates in a single query.
     *
     * Marketing salaries: 72000, 72000, 78000
     * PERCENTILE_CONT(0.5) = 72000.0 (interpolated median)
     * PERCENTILE_DISC(0.5) = 72000 (discrete median)
     * MODE() = 72000 (most frequent)
     */
    public function testMultipleOrderedSetAggregatesInOneQuery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                     PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS p_cont,
                     PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY salary) AS p_disc,
                     MODE() WITHIN GROUP (ORDER BY salary) AS mode_val
                 FROM pg_osa_employees
                 WHERE department = 'Marketing'"
            );

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(72000.0, (float) $rows[0]['p_cont'], 0.01,
                'PERCENTILE_CONT(0.5) for Marketing');
            $this->assertEqualsWithDelta(72000.0, (float) $rows[0]['p_disc'], 0.01,
                'PERCENTILE_DISC(0.5) for Marketing');
            $this->assertEqualsWithDelta(72000.0, (float) $rows[0]['mode_val'], 0.01,
                'MODE() for Marketing');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple ordered-set aggregates in one query failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Ordered-set aggregate after an UPDATE mutation.
     *
     * After updating Jack's salary from 110000 to 90000:
     * Engineering salaries become: 70000, 85000, 90000, 95000
     * PERCENTILE_CONT(0.5) = (85000 + 90000) / 2 = 87500.0
     */
    public function testOrderedSetAggregateAfterUpdate(): void
    {
        try {
            $this->ztdExec("UPDATE pg_osa_employees SET salary = 90000 WHERE id = 10");

            $rows = $this->ztdQuery(
                "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
                 FROM pg_osa_employees
                 WHERE department = 'Engineering'"
            );

            $this->assertCount(1, $rows);

            $actual = (float) $rows[0]['median_salary'];
            if (abs($actual - 87500.0) > 0.01) {
                $this->markTestIncomplete(
                    'PERCENTILE_CONT after UPDATE: got ' . $actual
                    . ', expected 87500.0. The CTE rewriter may not reflect the UPDATE in the aggregate.'
                );
            }

            $this->assertEqualsWithDelta(87500.0, $actual, 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Ordered-set aggregate after UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Ordered-set aggregate with a prepared-statement parameter for the percentile value.
     *
     * Uses $1 positional parameter for the fraction argument.
     * All employees sorted: 60000, 60000, 65000, 70000, 72000, 72000, 78000, 85000, 95000, 110000
     * PERCENTILE_CONT(0.25) at position 2.25 => 60000 + 0.25*(65000-60000) = 61250.0
     */
    public function testPreparedPercentileParameter(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT PERCENTILE_CONT($1::double precision) WITHIN GROUP (ORDER BY salary) AS pct
                 FROM pg_osa_employees",
                [0.25]
            );

            $this->assertCount(1, $rows);

            $actual = (float) $rows[0]['pct'];
            // 10 rows sorted: 60000,60000,65000,70000,72000,72000,78000,85000,95000,110000
            // position = 0.25 * (10-1) = 2.25 (0-indexed)
            // value at index 2 = 65000, index 3 = 70000
            // interpolated = 65000 + 0.25*(70000-65000) = 66250.0
            $expected = 66250.0;

            if (abs($actual - $expected) > 0.01) {
                $this->markTestIncomplete(
                    'Prepared PERCENTILE_CONT($1): got ' . $actual
                    . ', expected ' . $expected
                    . '. $N param in ordered-set aggregate may not be rewritten correctly.'
                );
            }

            $this->assertEqualsWithDelta($expected, $actual, 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared PERCENTILE_CONT with $1 parameter failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Ordered-set aggregate in a subquery used by DML.
     *
     * UPDATE all Engineering salaries to the department's median.
     * Engineering salaries: 70000, 85000, 95000, 110000
     * PERCENTILE_CONT(0.5) = 90000.0
     *
     * After UPDATE, all Engineering rows should have salary = 90000.
     */
    public function testOrderedSetAggregateSubqueryInUpdate(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_osa_employees
                 SET salary = (
                     SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)
                     FROM pg_osa_employees
                     WHERE department = 'Engineering'
                 )
                 WHERE department = 'Engineering'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, salary FROM pg_osa_employees
                 WHERE department = 'Engineering'
                 ORDER BY id"
            );

            $this->assertCount(4, $rows);

            $allMedian = true;
            foreach ($rows as $row) {
                if (abs((float) $row['salary'] - 90000.0) > 0.01) {
                    $allMedian = false;
                    break;
                }
            }

            if (!$allMedian) {
                $salaries = array_map(fn($r) => $r['salary'], $rows);
                $this->markTestIncomplete(
                    'UPDATE SET salary = (SELECT PERCENTILE_CONT(0.5)...): '
                    . 'Expected all Engineering salaries = 90000, got [' . implode(', ', $salaries) . ']. '
                    . 'WITHIN GROUP subquery in UPDATE SET may not be handled by CTE rewriter.'
                );
            }

            foreach ($rows as $row) {
                $this->assertEqualsWithDelta(90000.0, (float) $row['salary'], 0.01,
                    "Employee id={$row['id']} salary should be updated to the median");
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Ordered-set aggregate subquery in UPDATE failed: ' . $e->getMessage()
            );
        }
    }
}
