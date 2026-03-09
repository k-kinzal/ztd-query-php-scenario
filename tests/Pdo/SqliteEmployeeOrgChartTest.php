<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests employee org chart with self-joins (a table joined to itself).
 * SQL patterns exercised: self-join for employee-manager pairs, multi-level
 * hierarchy via LEFT JOIN, GROUP BY with self-join for counting direct reports,
 * NULL manager handling for root/CEO, subquery referencing same table,
 * prepared statement with self-join, shadow mutation visibility through self-join.
 * Self-joins stress the CTE rewriter because the same table appears twice with
 * different aliases, which could produce duplicate CTE names.
 * @spec SPEC-10.2.172
 */
class SqliteEmployeeOrgChartTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_eoc_employees (
            id INTEGER PRIMARY KEY,
            name TEXT,
            role TEXT,
            manager_id INTEGER
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_eoc_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_eoc_employees VALUES (1, 'Alice', 'CEO', NULL)");
        $this->pdo->exec("INSERT INTO sl_eoc_employees VALUES (2, 'Bob', 'VP Engineering', 1)");
        $this->pdo->exec("INSERT INTO sl_eoc_employees VALUES (3, 'Carol', 'VP Sales', 1)");
        $this->pdo->exec("INSERT INTO sl_eoc_employees VALUES (4, 'Dave', 'Senior Engineer', 2)");
        $this->pdo->exec("INSERT INTO sl_eoc_employees VALUES (5, 'Eve', 'Sales Rep', 3)");
    }

    /**
     * Self-join: pair each employee with their manager.
     * Alice (CEO) has NULL manager; Bob's manager is Alice; Dave's manager is Bob.
     */
    public function testSelfJoinEmployeeManager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, m.name AS manager_name
             FROM sl_eoc_employees e
             LEFT JOIN sl_eoc_employees m ON e.manager_id = m.id
             ORDER BY e.id"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertNull($rows[0]['manager_name']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Alice', $rows[1]['manager_name']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertSame('Alice', $rows[2]['manager_name']);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertSame('Bob', $rows[3]['manager_name']);

        $this->assertSame('Eve', $rows[4]['name']);
        $this->assertSame('Carol', $rows[4]['manager_name']);
    }

    /**
     * GROUP BY with self-join: count direct reports per manager.
     * Only managers with >0 reports: Alice(2), Bob(1), Carol(1).
     */
    public function testCountDirectReports(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, COUNT(e.id) AS direct_reports
             FROM sl_eoc_employees m
             LEFT JOIN sl_eoc_employees e ON e.manager_id = m.id
             GROUP BY m.id, m.name
             HAVING COUNT(e.id) > 0
             ORDER BY m.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['direct_reports']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['direct_reports']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['direct_reports']);
    }

    /**
     * Insert new employee via ztdExec, then self-join to verify
     * Frank shows up with Bob as manager.
     * Tests shadow store mutation visibility through self-join.
     */
    public function testSelfJoinWithUpdate(): void
    {
        $this->ztdExec(
            "INSERT INTO sl_eoc_employees VALUES (6, 'Frank', 'Engineer', 2)"
        );

        $rows = $this->ztdQuery(
            "SELECT e.name, m.name AS manager_name
             FROM sl_eoc_employees e
             LEFT JOIN sl_eoc_employees m ON e.manager_id = m.id
             WHERE e.id = 6"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
        $this->assertSame('Bob', $rows[0]['manager_name']);
    }

    /**
     * Prepared statement: find all employees whose manager has a given role.
     * Manager role = 'CEO' => Bob, Carol.
     */
    public function testSelfJoinWithPreparedStatement(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name
             FROM sl_eoc_employees e
             JOIN sl_eoc_employees m ON e.manager_id = m.id
             WHERE m.role = ?
             ORDER BY e.name",
            ['CEO']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Carol', $rows[1]['name']);
    }

    /**
     * Subquery referencing same table: find employees who are someone's manager.
     * Alice, Bob, Carol are managers.
     */
    public function testSubqueryReferencesSameTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name
             FROM sl_eoc_employees
             WHERE id IN (SELECT manager_id FROM sl_eoc_employees WHERE manager_id IS NOT NULL)
             ORDER BY name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Carol', $rows[2]['name']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec(
            "INSERT INTO sl_eoc_employees VALUES (6, 'Frank', 'Engineer', 2)"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_eoc_employees");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_eoc_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
