<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests hierarchical data queries using self-joins through ZTD shadow store.
 * Simulates an organizational chart / category tree without recursive CTEs.
 * @spec SPEC-3.3
 */
class MysqlHierarchicalSelfJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_hsj_employees (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                manager_id INT,
                department VARCHAR(50),
                salary DOUBLE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_hsj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // CEO (no manager)
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (1, 'Alice', NULL, 'Executive', 150000)");
        // VPs (report to CEO)
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (2, 'Bob', 1, 'Engineering', 120000)");
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (3, 'Charlie', 1, 'Sales', 110000)");
        // Managers (report to VPs)
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (4, 'Diana', 2, 'Engineering', 90000)");
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (5, 'Eve', 2, 'Engineering', 85000)");
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (6, 'Frank', 3, 'Sales', 80000)");
        // Individual contributors (report to managers)
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (7, 'Grace', 4, 'Engineering', 70000)");
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (8, 'Heidi', 4, 'Engineering', 72000)");
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (9, 'Ivan', 5, 'Engineering', 68000)");
        $this->pdo->exec("INSERT INTO mp_hsj_employees VALUES (10, 'Judy', 6, 'Sales', 65000)");
    }

    /**
     * List employees with their manager name (1-level self-join).
     */
    public function testEmployeeWithManager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager
             FROM mp_hsj_employees e
             LEFT JOIN mp_hsj_employees m ON e.manager_id = m.id
             ORDER BY e.name"
        );

        $this->assertCount(10, $rows);
        // Alice has no manager
        $alice = array_values(array_filter($rows, fn($r) => $r['employee'] === 'Alice'));
        $this->assertNull($alice[0]['manager']);
        // Diana reports to Bob
        $diana = array_values(array_filter($rows, fn($r) => $r['employee'] === 'Diana'));
        $this->assertSame('Bob', $diana[0]['manager']);
    }

    /**
     * List employees with their grandmanager (2-level self-join).
     */
    public function testEmployeeWithGrandmanager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager, gm.name AS grandmanager
             FROM mp_hsj_employees e
             LEFT JOIN mp_hsj_employees m ON e.manager_id = m.id
             LEFT JOIN mp_hsj_employees gm ON m.manager_id = gm.id
             WHERE gm.id IS NOT NULL
             ORDER BY e.name"
        );

        // Employees with a grandmanager: Diana, Eve, Frank (their GMs are Alice), Grace, Heidi, Ivan, Judy
        $this->assertCount(7, $rows);
        $grace = array_values(array_filter($rows, fn($r) => $r['employee'] === 'Grace'));
        $this->assertSame('Diana', $grace[0]['manager']);
        $this->assertSame('Bob', $grace[0]['grandmanager']);
    }

    /**
     * Count direct reports per manager.
     */
    public function testDirectReportCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name AS manager, COUNT(e.id) AS direct_reports
             FROM mp_hsj_employees m
             JOIN mp_hsj_employees e ON e.manager_id = m.id
             GROUP BY m.id, m.name
             ORDER BY direct_reports DESC, m.name"
        );

        $this->assertCount(6, $rows); // 6 people have direct reports
        // Bob has 2 direct reports (Diana, Eve)
        $bob = array_values(array_filter($rows, fn($r) => $r['manager'] === 'Bob'));
        $this->assertEquals(2, (int) $bob[0]['direct_reports']);
        // Alice has 2 direct reports (Bob, Charlie)
        $alice = array_values(array_filter($rows, fn($r) => $r['manager'] === 'Alice'));
        $this->assertEquals(2, (int) $alice[0]['direct_reports']);
    }

    /**
     * Total salary budget per manager (including their own).
     */
    public function testTeamSalaryBudget(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name AS manager,
                    m.salary AS own_salary,
                    SUM(e.salary) AS team_salary,
                    m.salary + SUM(e.salary) AS total_budget
             FROM mp_hsj_employees m
             JOIN mp_hsj_employees e ON e.manager_id = m.id
             GROUP BY m.id, m.name, m.salary
             ORDER BY total_budget DESC"
        );

        $this->assertCount(6, $rows);
        // Alice's direct team: Bob(120k) + Charlie(110k) + own(150k) = 380k
        $this->assertSame('Alice', $rows[0]['manager']);
        $this->assertEqualsWithDelta(380000.0, (float) $rows[0]['total_budget'], 0.01);
    }

    /**
     * Find employees who earn more than their manager (self-join with comparison).
     */
    public function testEmployeesEarningMoreThanManager(): void
    {
        // Give Grace a raise that exceeds her manager Diana's salary
        $this->pdo->exec("UPDATE mp_hsj_employees SET salary = 95000 WHERE id = 7");

        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, e.salary AS emp_salary,
                    m.name AS manager, m.salary AS mgr_salary
             FROM mp_hsj_employees e
             JOIN mp_hsj_employees m ON e.manager_id = m.id
             WHERE e.salary > m.salary
             ORDER BY e.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Grace', $rows[0]['employee']);
        $this->assertEqualsWithDelta(95000.0, (float) $rows[0]['emp_salary'], 0.01);
        $this->assertSame('Diana', $rows[0]['manager']);
    }

    /**
     * Find the root of the tree (employees with no manager).
     */
    public function testFindRootNodes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, department FROM mp_hsj_employees WHERE manager_id IS NULL"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * Find leaf nodes (employees with no direct reports).
     */
    public function testFindLeafNodes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name
             FROM mp_hsj_employees e
             WHERE NOT EXISTS (
                 SELECT 1 FROM mp_hsj_employees sub
                 WHERE sub.manager_id = e.id
             )
             ORDER BY e.name"
        );

        $this->assertCount(4, $rows); // Grace, Heidi, Ivan, Judy
        $names = array_column($rows, 'name');
        $this->assertContains('Grace', $names);
        $this->assertContains('Heidi', $names);
        $this->assertContains('Ivan', $names);
        $this->assertContains('Judy', $names);
    }

    /**
     * Department salary summary with rank.
     */
    public function testDepartmentSalaryRank(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, department, salary,
                    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank
             FROM mp_hsj_employees
             WHERE department = 'Engineering'
             ORDER BY dept_rank, name"
        );

        $this->assertGreaterThan(0, count($rows));
        // Bob is highest paid in Engineering
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['dept_rank']);
    }

    /**
     * Promote an employee (change manager) and verify hierarchy update.
     */
    public function testPromoteAndReverify(): void
    {
        // Promote Diana to report directly to CEO (Alice)
        $this->pdo->exec("UPDATE mp_hsj_employees SET manager_id = 1 WHERE id = 4");

        // Diana's manager should now be Alice
        $rows = $this->ztdQuery(
            "SELECT e.name, m.name AS manager
             FROM mp_hsj_employees e
             JOIN mp_hsj_employees m ON e.manager_id = m.id
             WHERE e.id = 4"
        );
        $this->assertSame('Alice', $rows[0]['manager']);

        // Alice now has 3 direct reports
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mp_hsj_employees WHERE manager_id = 1"
        );
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Prepared statement: find all employees under a given manager.
     */
    public function testPreparedDirectReportsLookup(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT name FROM mp_hsj_employees WHERE manager_id = ? ORDER BY name'
        );

        // Bob's reports
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $rows);
        $this->assertSame('Diana', $rows[0]);
        $this->assertSame('Eve', $rows[1]);
    }
}
