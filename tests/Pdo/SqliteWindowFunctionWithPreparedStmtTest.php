<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests window functions with prepared statements on SQLite.
 *
 * Window functions combined with parameter binding are common
 * in dynamic analytics queries. Tests whether the CTE rewriter
 * handles window function queries via the prepare path.
 * @spec pending
 */
class SqliteWindowFunctionWithPreparedStmtTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_wfprep_test (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sl_wfprep_test'];
    }


    /**
     * ROW_NUMBER with WHERE parameter.
     */
    public function testRowNumberWithWhereParam(): void
    {
        $stmt = $this->pdo->prepare('
            SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
            FROM sl_wfprep_test
            WHERE dept = ?
        ');
        $stmt->execute(['Engineering']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['rn']);
    }

    /**
     * RANK with parameter in WHERE.
     */
    public function testRankWithWhereParam(): void
    {
        $stmt = $this->pdo->prepare('
            SELECT name, RANK() OVER (ORDER BY salary DESC) AS rnk
            FROM sl_wfprep_test
            WHERE salary > ?
        ');
        $stmt->execute([75000]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Eve(95k), Alice(90k), Bob(85k)
    }

    /**
     * SUM window with PARTITION BY.
     */
    public function testSumWindowWithPartitionBy(): void
    {
        $stmt = $this->pdo->prepare('
            SELECT name, dept, salary,
                   SUM(salary) OVER (PARTITION BY dept) AS dept_total
            FROM sl_wfprep_test
            WHERE salary >= ?
            ORDER BY name
        ');
        $stmt->execute([70000]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
    }

    /**
     * Multiple window functions in single prepared query.
     */
    public function testMultipleWindowFunctions(): void
    {
        $stmt = $this->pdo->prepare('
            SELECT name,
                   ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
                   RANK() OVER (ORDER BY salary DESC) AS rnk,
                   SUM(salary) OVER () AS total_salary
            FROM sl_wfprep_test
            WHERE dept = ?
        ');
        $stmt->execute(['Engineering']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame(1, (int) $rows[0]['rn']);
        $this->assertSame(1, (int) $rows[0]['rnk']);
    }

    /**
     * Window function after INSERT mutation.
     */
    public function testWindowFunctionAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_wfprep_test VALUES (6, 'Frank', 'Engineering', 100000)");

        $stmt = $this->pdo->prepare('
            SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
            FROM sl_wfprep_test
            WHERE dept = ?
        ');
        $stmt->execute(['Engineering']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_wfprep_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
