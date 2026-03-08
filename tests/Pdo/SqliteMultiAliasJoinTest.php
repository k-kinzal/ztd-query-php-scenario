<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests same table referenced multiple times with different aliases.
 * The CTE rewriter must produce a single CTE for the table and correctly
 * resolve all aliases pointing to it. Fragile when the rewriter generates
 * one CTE per alias or fails to substitute all alias references.
 * @spec SPEC-3.3
 */
class SqliteMultiAliasJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_maj_employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                manager_id INTEGER,
                mentor_id INTEGER,
                dept TEXT,
                salary INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_maj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_maj_employees VALUES (1, 'Alice',   NULL, NULL, 'exec', 200)");
        $this->pdo->exec("INSERT INTO sl_maj_employees VALUES (2, 'Bob',     1,    NULL, 'exec', 150)");
        $this->pdo->exec("INSERT INTO sl_maj_employees VALUES (3, 'Charlie', 2,    1,    'eng',  120)");
        $this->pdo->exec("INSERT INTO sl_maj_employees VALUES (4, 'Diana',   2,    1,    'eng',  110)");
        $this->pdo->exec("INSERT INTO sl_maj_employees VALUES (5, 'Eve',     3,    2,    'eng',  100)");
        $this->pdo->exec("INSERT INTO sl_maj_employees VALUES (6, 'Frank',   3,    2,    'eng',   95)");
    }

    /**
     * Basic self-join: employee with manager name.
     */
    public function testSelfJoinTwoAliases(): void
    {
        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM sl_maj_employees a
            JOIN sl_maj_employees b ON a.manager_id = b.id
            ORDER BY a.name
        ");
        $this->assertCount(5, $rows);
        $this->assertSame('Bob', $rows[0]['employee']);
        $this->assertSame('Alice', $rows[0]['manager']);
        $this->assertSame('Diana', $rows[2]['employee']);
        $this->assertSame('Bob', $rows[2]['manager']);
    }

    /**
     * Triple self-join: employee, manager, and mentor from the same table.
     * The CTE rewriter must handle three aliases of sl_maj_employees correctly.
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                e.name AS employee,
                m.name AS manager,
                t.name AS mentor
            FROM sl_maj_employees e
            LEFT JOIN sl_maj_employees m ON e.manager_id = m.id
            LEFT JOIN sl_maj_employees t ON e.mentor_id = t.id
            ORDER BY e.id
        ");
        $this->assertCount(6, $rows);

        // Alice: no manager, no mentor
        $this->assertSame('Alice', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
        $this->assertNull($rows[0]['mentor']);

        // Charlie: manager=Bob, mentor=Alice
        $this->assertSame('Charlie', $rows[2]['employee']);
        $this->assertSame('Bob', $rows[2]['manager']);
        $this->assertSame('Alice', $rows[2]['mentor']);

        // Eve: manager=Charlie, mentor=Bob
        $this->assertSame('Eve', $rows[4]['employee']);
        $this->assertSame('Charlie', $rows[4]['manager']);
        $this->assertSame('Bob', $rows[4]['mentor']);
    }

    /**
     * Self-join after mutation: UPDATE one row, then self-join should reflect
     * the change through all alias references.
     */
    public function testSelfJoinAfterUpdate(): void
    {
        // Promote Charlie: change name and salary
        $this->pdo->exec("UPDATE sl_maj_employees SET name = 'Charles', salary = 160 WHERE id = 3");

        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM sl_maj_employees a
            JOIN sl_maj_employees b ON a.manager_id = b.id
            WHERE a.id = 5
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['employee']);
        // Manager name should reflect the update
        $this->assertSame('Charles', $rows[0]['manager']);
    }

    /**
     * Self-join after INSERT: add a new employee, self-join should pick it up
     * both as an employee and as a potential manager reference.
     */
    public function testSelfJoinAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_maj_employees VALUES (7, 'Grace', 5, 3, 'eng', 90)");

        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM sl_maj_employees a
            JOIN sl_maj_employees b ON a.manager_id = b.id
            WHERE a.id = 7
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Grace', $rows[0]['employee']);
        $this->assertSame('Eve', $rows[0]['manager']);
    }

    /**
     * Self-join with aggregation: GROUP BY on one alias, SUM from another.
     * Count how many direct reports each manager has plus their total salary.
     */
    public function testSelfJoinWithAggregation(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                m.name AS manager,
                COUNT(e.id) AS report_count,
                SUM(e.salary) AS total_report_salary
            FROM sl_maj_employees e
            JOIN sl_maj_employees m ON e.manager_id = m.id
            GROUP BY m.id, m.name
            ORDER BY report_count DESC
        ");
        // Alice manages Bob (1 report)
        // Bob manages Charlie, Diana (2 reports)
        // Charlie manages Eve, Frank (2 reports)
        $this->assertCount(3, $rows);

        $byManager = array_column($rows, null, 'manager');
        $this->assertEquals(2, (int) $byManager['Bob']['report_count']);
        $this->assertEquals(230, (int) $byManager['Bob']['total_report_salary']); // 120+110
        $this->assertEquals(2, (int) $byManager['Charlie']['report_count']);
        $this->assertEquals(195, (int) $byManager['Charlie']['total_report_salary']); // 100+95
        $this->assertEquals(1, (int) $byManager['Alice']['report_count']);
    }

    /**
     * Self-join with WHERE filter on both aliases.
     */
    public function testSelfJoinFilterBothAliases(): void
    {
        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM sl_maj_employees a
            JOIN sl_maj_employees b ON a.manager_id = b.id
            WHERE a.dept = 'eng' AND b.dept = 'exec'
            ORDER BY a.name
        ");
        // Charlie and Diana report to Bob (exec)
        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['employee']);
        $this->assertSame('Bob', $rows[0]['manager']);
        $this->assertSame('Diana', $rows[1]['employee']);
        $this->assertSame('Bob', $rows[1]['manager']);
    }

    /**
     * Self-join with subquery: find employees whose salary exceeds their
     * manager's salary (using self-join, not correlated subquery).
     */
    public function testSelfJoinSalaryComparison(): void
    {
        $rows = $this->ztdQuery("
            SELECT a.name AS employee, a.salary AS emp_salary,
                   b.name AS manager, b.salary AS mgr_salary
            FROM sl_maj_employees a
            JOIN sl_maj_employees b ON a.manager_id = b.id
            WHERE a.salary > b.salary
            ORDER BY a.name
        ");
        // No employee earns more than their manager in our data
        $this->assertCount(0, $rows);
    }

    /**
     * Self-join after DELETE: remove a manager, then self-join should
     * reflect the deletion (join should produce no row for orphaned employees).
     */
    public function testSelfJoinAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM sl_maj_employees WHERE id = 3");

        // INNER JOIN: Eve and Frank had manager_id=3, which is now deleted
        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM sl_maj_employees a
            JOIN sl_maj_employees b ON a.manager_id = b.id
            ORDER BY a.name
        ");
        // Bob->Alice, Diana->Bob remain; Eve->Charlie and Frank->Charlie are gone
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['employee']);
        $this->assertSame('Diana', $rows[1]['employee']);
    }

    /**
     * Four aliases of the same table: employee, manager, mentor, and
     * manager's manager (skip-level).
     */
    public function testQuadrupleAliasSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                e.name AS employee,
                m.name AS manager,
                mm.name AS grand_manager,
                t.name AS mentor
            FROM sl_maj_employees e
            LEFT JOIN sl_maj_employees m ON e.manager_id = m.id
            LEFT JOIN sl_maj_employees mm ON m.manager_id = mm.id
            LEFT JOIN sl_maj_employees t ON e.mentor_id = t.id
            WHERE e.id = 5
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['employee']);
        $this->assertSame('Charlie', $rows[0]['manager']);
        $this->assertSame('Bob', $rows[0]['grand_manager']);
        $this->assertSame('Bob', $rows[0]['mentor']);
    }
}
