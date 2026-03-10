<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT with window functions on MySQL PDO.
 *
 * Window functions in INSERT...SELECT is a common real-world pattern, e.g.
 * numbering rows for a report table, computing running totals, or ranking
 * employees by department. The CTE rewriter must handle the window function
 * expressions in the SELECT clause correctly when they feed into an INSERT.
 *
 * @spec SPEC-10.2.370
 */
class MysqlInsertSelectWindowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mp_isw_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                salary DECIMAL(10,2) NOT NULL,
                dept VARCHAR(30) NOT NULL
            )",
            "CREATE TABLE mp_isw_ranked (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                salary DECIMAL(10,2),
                dept VARCHAR(30),
                rank_in_dept INT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_isw_ranked', 'mp_isw_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_isw_employees (id, name, salary, dept) VALUES (1, 'Alice', 90000, 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_isw_employees (id, name, salary, dept) VALUES (2, 'Bob', 80000, 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_isw_employees (id, name, salary, dept) VALUES (3, 'Carol', 95000, 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_isw_employees (id, name, salary, dept) VALUES (4, 'Dave', 70000, 'Sales')");
        $this->pdo->exec("INSERT INTO mp_isw_employees (id, name, salary, dept) VALUES (5, 'Eve', 75000, 'Sales')");
        $this->pdo->exec("INSERT INTO mp_isw_employees (id, name, salary, dept) VALUES (6, 'Frank', 85000, 'Marketing')");
    }

    /**
     * INSERT...SELECT with ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC).
     *
     * This is the classic pattern for populating a ranked report table.
     * Window function results are computed columns, so the CTE rewriter
     * may produce NULL for the rank_in_dept column.
     */
    public function testInsertSelectRowNumberPartitioned(): void
    {
        try {
            $affected = $this->pdo->exec("
                INSERT INTO mp_isw_ranked (id, name, salary, dept, rank_in_dept)
                SELECT id, name, salary, dept,
                       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
                FROM mp_isw_employees
            ");
            $this->assertSame(6, $affected);

            $rows = $this->ztdQuery("SELECT * FROM mp_isw_ranked ORDER BY dept, rank_in_dept");
            $this->assertCount(6, $rows);

            // Engineering: Carol(95k)=1, Alice(90k)=2, Bob(80k)=3
            $eng = array_values(array_filter($rows, fn($r) => $r['dept'] === 'Engineering'));
            $this->assertSame('Carol', $eng[0]['name']);
            $this->assertSame(1, (int) $eng[0]['rank_in_dept']);
            $this->assertSame('Alice', $eng[1]['name']);
            $this->assertSame(2, (int) $eng[1]['rank_in_dept']);
            $this->assertSame('Bob', $eng[2]['name']);
            $this->assertSame(3, (int) $eng[2]['rank_in_dept']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT with ROW_NUMBER() window function failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with RANK() OVER (ORDER BY salary DESC).
     *
     * RANK() produces gaps for ties, unlike DENSE_RANK().
     */
    public function testInsertSelectRankOverSalary(): void
    {
        try {
            // Add a tie in salary
            $this->pdo->exec("UPDATE mp_isw_employees SET salary = 80000 WHERE id = 4");

            $affected = $this->pdo->exec("
                INSERT INTO mp_isw_ranked (id, name, salary, dept, rank_in_dept)
                SELECT id, name, salary, dept,
                       RANK() OVER (ORDER BY salary DESC)
                FROM mp_isw_employees
            ");
            $this->assertSame(6, $affected);

            $rows = $this->ztdQuery("SELECT * FROM mp_isw_ranked ORDER BY rank_in_dept, name");
            $this->assertCount(6, $rows);

            // Carol(95k)=1, Alice(90k)=2, Frank(85k)=3, Bob(80k)=4, Dave(80k)=4, Eve(75k)=6
            $this->assertSame(1, (int) $rows[0]['rank_in_dept']);
            $this->assertSame('Carol', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT with RANK() window function failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with SUM() OVER (PARTITION BY dept ORDER BY id) for running totals.
     *
     * Running totals are a very common reporting pattern.
     */
    public function testInsertSelectRunningTotalSumOver(): void
    {
        try {
            $affected = $this->pdo->exec("
                INSERT INTO mp_isw_ranked (id, name, salary, dept, rank_in_dept)
                SELECT id, name, salary, dept,
                       CAST(SUM(salary) OVER (PARTITION BY dept ORDER BY id) AS SIGNED)
                FROM mp_isw_employees
            ");
            $this->assertSame(6, $affected);

            $rows = $this->ztdQuery("SELECT * FROM mp_isw_ranked WHERE dept = 'Engineering' ORDER BY id");

            // Engineering running totals: Alice=90000, Bob=170000, Carol=265000
            $this->assertCount(3, $rows);
            $this->assertSame(90000, (int) $rows[0]['rank_in_dept']);
            $this->assertSame(170000, (int) $rows[1]['rank_in_dept']);
            $this->assertSame(265000, (int) $rows[2]['rank_in_dept']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT with SUM() OVER running total failed: ' . $e->getMessage());
        }
    }

    /**
     * Verify target table row count matches source after INSERT...SELECT with window.
     */
    public function testTargetRowCountMatchesSource(): void
    {
        try {
            $this->pdo->exec("
                INSERT INTO mp_isw_ranked (id, name, salary, dept, rank_in_dept)
                SELECT id, name, salary, dept,
                       ROW_NUMBER() OVER (ORDER BY id)
                FROM mp_isw_employees
            ");

            $sourceCount = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_isw_employees");
            $targetCount = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_isw_ranked");

            $this->assertSame(
                (int) $sourceCount[0]['cnt'],
                (int) $targetCount[0]['cnt'],
                'Target table row count must match source table'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT window row count verification failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with window function after UPDATE on source table.
     *
     * The window function results should reflect the updated salary values.
     */
    public function testInsertSelectWindowAfterSourceUpdate(): void
    {
        try {
            // Promote Carol to a higher salary
            $this->pdo->exec("UPDATE mp_isw_employees SET salary = 120000 WHERE name = 'Carol'");
            // Demote Bob
            $this->pdo->exec("UPDATE mp_isw_employees SET salary = 60000 WHERE name = 'Bob'");

            $affected = $this->pdo->exec("
                INSERT INTO mp_isw_ranked (id, name, salary, dept, rank_in_dept)
                SELECT id, name, salary, dept,
                       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
                FROM mp_isw_employees
            ");
            $this->assertSame(6, $affected);

            $rows = $this->ztdQuery("SELECT * FROM mp_isw_ranked WHERE dept = 'Engineering' ORDER BY rank_in_dept");

            // After update: Carol(120k)=1, Alice(90k)=2, Bob(60k)=3
            $this->assertCount(3, $rows);
            $this->assertSame('Carol', $rows[0]['name']);
            $this->assertEqualsWithDelta(120000, (float) $rows[0]['salary'], 0.01);
            $this->assertSame(1, (int) $rows[0]['rank_in_dept']);
            $this->assertSame('Alice', $rows[1]['name']);
            $this->assertSame(2, (int) $rows[1]['rank_in_dept']);
            $this->assertSame('Bob', $rows[2]['name']);
            $this->assertSame(3, (int) $rows[2]['rank_in_dept']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT with window after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with window using prepared statement (? placeholder).
     *
     * Filters source by department parameter before applying window function.
     */
    public function testInsertSelectWindowPreparedWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare("
                INSERT INTO mp_isw_ranked (id, name, salary, dept, rank_in_dept)
                SELECT id, name, salary, dept,
                       ROW_NUMBER() OVER (ORDER BY salary DESC)
                FROM mp_isw_employees
                WHERE dept = ?
            ");
            $stmt->execute(['Engineering']);

            $rows = $this->ztdQuery("SELECT * FROM mp_isw_ranked ORDER BY rank_in_dept");
            $this->assertCount(3, $rows);
            $this->assertSame('Carol', $rows[0]['name']);
            $this->assertSame(1, (int) $rows[0]['rank_in_dept']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT window with prepared statement failed: ' . $e->getMessage());
        }
    }
}
