<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multiple self-joins (same table joined to itself multiple times)
 * through CTE shadow store.
 *
 * Self-joins are common for hierarchical data, comparison queries, and
 * finding pairs. The CTE rewriter must correctly handle multiple aliases
 * for the same table in a single query.
 *
 * @spec SPEC-3.2
 */
class SqliteMultiSelfJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_msj_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                manager_id INTEGER,
                department TEXT NOT NULL,
                salary REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_msj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (1, 'CEO', NULL, 'Executive', 200000)");
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (2, 'VP Eng', 1, 'Engineering', 150000)");
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (3, 'VP Sales', 1, 'Sales', 140000)");
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (4, 'Dev Lead', 2, 'Engineering', 120000)");
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (5, 'Sr Dev', 4, 'Engineering', 100000)");
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (6, 'Jr Dev', 4, 'Engineering', 70000)");
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (7, 'Sales Rep', 3, 'Sales', 80000)");
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (8, 'Sales Rep 2', 3, 'Sales', 75000)");
    }

    /**
     * Simple self-join: employee with manager name.
     */
    public function testEmployeeWithManager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager
             FROM sl_msj_employees e
             LEFT JOIN sl_msj_employees m ON m.id = e.manager_id
             ORDER BY e.id"
        );

        $this->assertCount(8, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
        $this->assertSame('VP Eng', $rows[1]['employee']);
        $this->assertSame('CEO', $rows[1]['manager']);
        $this->assertSame('Jr Dev', $rows[5]['employee']);
        $this->assertSame('Dev Lead', $rows[5]['manager']);
    }

    /**
     * Two-level self-join: employee, manager, skip-level manager.
     */
    public function testTwoLevelSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee,
                    m.name AS manager,
                    gm.name AS grand_manager
             FROM sl_msj_employees e
             LEFT JOIN sl_msj_employees m ON m.id = e.manager_id
             LEFT JOIN sl_msj_employees gm ON gm.id = m.manager_id
             WHERE gm.id IS NOT NULL
             ORDER BY e.name"
        );

        // Employees with a grand-manager: Dev Lead (→VP Eng→CEO), Jr Dev (→Dev Lead→VP Eng),
        // Sales Rep (→VP Sales→CEO), Sales Rep 2 (→VP Sales→CEO), Sr Dev (→Dev Lead→VP Eng)
        $this->assertCount(5, $rows);
        $this->assertSame('Dev Lead', $rows[0]['employee']);
        $this->assertSame('VP Eng', $rows[0]['manager']);
        $this->assertSame('CEO', $rows[0]['grand_manager']);
    }

    /**
     * Self-join for comparison: find employees who earn more than their manager.
     */
    public function testSelfJoinComparison(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, e.salary, m.name AS manager, m.salary AS mgr_salary
             FROM sl_msj_employees e
             JOIN sl_msj_employees m ON m.id = e.manager_id
             WHERE e.salary > m.salary
             ORDER BY e.name"
        );

        // No one earns more than their manager in this data
        $this->assertCount(0, $rows);
    }

    /**
     * Self-join for pairs: find all pairs of employees in the same department.
     */
    public function testSelfJoinPairs(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e1.name AS emp1, e2.name AS emp2
             FROM sl_msj_employees e1
             JOIN sl_msj_employees e2 ON e1.department = e2.department AND e1.id < e2.id
             WHERE e1.department = 'Engineering'
             ORDER BY e1.name, e2.name"
        );

        // Engineering has 4 people: VP Eng, Dev Lead, Sr Dev, Jr Dev → 6 pairs
        $this->assertCount(6, $rows);
    }

    /**
     * Self-join with aggregate: count direct reports per manager.
     */
    public function testSelfJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name AS manager, COUNT(e.id) AS direct_reports
             FROM sl_msj_employees m
             LEFT JOIN sl_msj_employees e ON e.manager_id = m.id
             GROUP BY m.id, m.name
             HAVING COUNT(e.id) > 0
             ORDER BY direct_reports DESC, m.name"
        );

        $this->assertCount(4, $rows);
        // CEO: 2 reports, VP Eng: 1, VP Sales: 2, Dev Lead: 2
        $this->assertSame('CEO', $rows[0]['manager']);
        $this->assertEquals(2, (int) $rows[0]['direct_reports']);
    }

    /**
     * Self-join for rank comparison: find employees who earn the most in their dept.
     */
    public function testSelfJoinNobodyEarnsMore(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, e.department, e.salary
             FROM sl_msj_employees e
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_msj_employees e2
                 WHERE e2.department = e.department AND e2.salary > e.salary
             )
             ORDER BY e.department"
        );

        // CEO (Executive), VP Eng (Engineering), VP Sales (Sales)
        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('VP Eng', $rows[0]['name']);
        $this->assertSame('Executive', $rows[1]['department']);
        $this->assertSame('CEO', $rows[1]['name']);
        $this->assertSame('Sales', $rows[2]['department']);
        $this->assertSame('VP Sales', $rows[2]['name']);
    }

    /**
     * Triple self-join: three levels of hierarchy in one query.
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee,
                    m1.name AS level1,
                    m2.name AS level2,
                    m3.name AS level3
             FROM sl_msj_employees e
             LEFT JOIN sl_msj_employees m1 ON m1.id = e.manager_id
             LEFT JOIN sl_msj_employees m2 ON m2.id = m1.manager_id
             LEFT JOIN sl_msj_employees m3 ON m3.id = m2.manager_id
             WHERE m3.id IS NOT NULL
             ORDER BY e.name"
        );

        // Employees with 3 levels up: Jr Dev (→Dev Lead→VP Eng→CEO), Sr Dev (→Dev Lead→VP Eng→CEO)
        $this->assertCount(2, $rows);
        $this->assertSame('Jr Dev', $rows[0]['employee']);
        $this->assertSame('Dev Lead', $rows[0]['level1']);
        $this->assertSame('VP Eng', $rows[0]['level2']);
        $this->assertSame('CEO', $rows[0]['level3']);
    }

    /**
     * Self-join after mutation: adding a new employee and verifying hierarchy.
     */
    public function testSelfJoinAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_msj_employees VALUES (9, 'Intern', 6, 'Engineering', 40000)");

        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager
             FROM sl_msj_employees e
             JOIN sl_msj_employees m ON m.id = e.manager_id
             WHERE e.id = 9"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Intern', $rows[0]['employee']);
        $this->assertSame('Jr Dev', $rows[0]['manager']);
    }

    /**
     * Prepared self-join with department filter.
     */
    public function testPreparedSelfJoin(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name AS employee, m.name AS manager
             FROM sl_msj_employees e
             LEFT JOIN sl_msj_employees m ON m.id = e.manager_id
             WHERE e.department = ?
             ORDER BY e.salary DESC",
            ['Engineering']
        );

        $this->assertCount(4, $rows);
        $this->assertSame('VP Eng', $rows[0]['employee']);
        $this->assertSame('CEO', $rows[0]['manager']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_msj_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
