<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests triple and quadruple self-joins on a single table through ZTD on PostgreSQL via PDO.
 * Self-joins are common for hierarchical queries (employee-manager chains).
 * @spec SPEC-10.2.99
 */
class PostgresTripleSelfJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_tsj_employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            manager_id INTEGER,
            department VARCHAR(255),
            salary NUMERIC(10,2)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_tsj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Hierarchy: CEO -> VP_Eng -> Alice -> Dave
        //            CEO -> VP_Sales -> Bob
        //            CEO -> VP_Eng -> Charlie
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (1, 'CEO', NULL, 'Executive', 300000.00)");
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (2, 'VP_Eng', 1, 'Engineering', 200000.00)");
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (3, 'VP_Sales', 1, 'Sales', 190000.00)");
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (4, 'Alice', 2, 'Engineering', 120000.00)");
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (5, 'Bob', 3, 'Sales', 110000.00)");
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (6, 'Charlie', 2, 'Engineering', 115000.00)");
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (7, 'Dave', 4, 'Engineering', 95000.00)");
    }

    /**
     * Triple self-join: employee -> manager -> grand-manager (3 levels).
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee,
                    m.name AS manager,
                    gm.name AS grand_manager
             FROM pg_tsj_employees e
             JOIN pg_tsj_employees m ON e.manager_id = m.id
             JOIN pg_tsj_employees gm ON m.manager_id = gm.id
             ORDER BY e.name"
        );

        // Employees with both manager and grand-manager:
        // Alice -> VP_Eng -> CEO
        // Bob -> VP_Sales -> CEO
        // Charlie -> VP_Eng -> CEO
        // Dave -> Alice -> VP_Eng
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['employee']);
        $this->assertSame('VP_Eng', $rows[0]['manager']);
        $this->assertSame('CEO', $rows[0]['grand_manager']);
        $this->assertSame('Bob', $rows[1]['employee']);
        $this->assertSame('VP_Sales', $rows[1]['manager']);
        $this->assertSame('CEO', $rows[1]['grand_manager']);
        $this->assertSame('Charlie', $rows[2]['employee']);
        $this->assertSame('VP_Eng', $rows[2]['manager']);
        $this->assertSame('CEO', $rows[2]['grand_manager']);
        $this->assertSame('Dave', $rows[3]['employee']);
        $this->assertSame('Alice', $rows[3]['manager']);
        $this->assertSame('VP_Eng', $rows[3]['grand_manager']);
    }

    /**
     * Quadruple self-join: employee -> manager -> grand-manager -> great-grand-manager.
     */
    public function testQuadrupleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee,
                    m1.name AS manager,
                    m2.name AS grand_manager,
                    m3.name AS great_grand_manager
             FROM pg_tsj_employees e
             JOIN pg_tsj_employees m1 ON e.manager_id = m1.id
             JOIN pg_tsj_employees m2 ON m1.manager_id = m2.id
             JOIN pg_tsj_employees m3 ON m2.manager_id = m3.id
             ORDER BY e.name"
        );

        // Only Dave has 3 levels of management: Dave -> Alice -> VP_Eng -> CEO
        $this->assertCount(1, $rows);
        $this->assertSame('Dave', $rows[0]['employee']);
        $this->assertSame('Alice', $rows[0]['manager']);
        $this->assertSame('VP_Eng', $rows[0]['grand_manager']);
        $this->assertSame('CEO', $rows[0]['great_grand_manager']);
    }

    /**
     * Self-join with aggregate: count direct reports per manager.
     */
    public function testSelfJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name AS manager,
                    COUNT(e.id) AS direct_reports,
                    SUM(e.salary) AS team_salary
             FROM pg_tsj_employees m
             JOIN pg_tsj_employees e ON e.manager_id = m.id
             GROUP BY m.name
             ORDER BY direct_reports DESC, m.name"
        );

        // CEO: 2 reports (VP_Eng, VP_Sales)
        // VP_Eng: 2 reports (Alice, Charlie)
        // VP_Sales: 1 report (Bob)
        // Alice: 1 report (Dave)
        $this->assertCount(4, $rows);
        $this->assertSame('CEO', $rows[0]['manager']);
        $this->assertEquals(2, (int) $rows[0]['direct_reports']);
        $this->assertSame('VP_Eng', $rows[1]['manager']);
        $this->assertEquals(2, (int) $rows[1]['direct_reports']);
        $this->assertSame('Alice', $rows[2]['manager']);
        $this->assertEquals(1, (int) $rows[2]['direct_reports']);
        $this->assertSame('VP_Sales', $rows[3]['manager']);
        $this->assertEquals(1, (int) $rows[3]['direct_reports']);
    }

    /**
     * Self-join with correlated subquery: employees earning more than their manager.
     */
    public function testSelfJoinWithSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, e.salary, m.name AS manager_name, m.salary AS manager_salary
             FROM pg_tsj_employees e
             JOIN pg_tsj_employees m ON e.manager_id = m.id
             WHERE e.salary > m.salary
             ORDER BY e.name"
        );

        // No one earns more than their manager in this dataset
        $this->assertCount(0, $rows);

        // Add an overpaid employee
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (8, 'Star', 6, 'Engineering', 200000.00)");

        $rows = $this->ztdQuery(
            "SELECT e.name, e.salary, m.name AS manager_name, m.salary AS manager_salary
             FROM pg_tsj_employees e
             JOIN pg_tsj_employees m ON e.manager_id = m.id
             WHERE e.salary > m.salary
             ORDER BY e.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Star', $rows[0]['name']);
        $this->assertEquals(200000.00, (float) $rows[0]['salary']);
        $this->assertSame('Charlie', $rows[0]['manager_name']);
        $this->assertEquals(115000.00, (float) $rows[0]['manager_salary']);
    }

    /**
     * Self-join after a mutation (salary update).
     */
    public function testSelfJoinAfterMutation(): void
    {
        $this->pdo->exec("UPDATE pg_tsj_employees SET salary = 250000.00 WHERE name = 'Alice'");

        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, e.salary, m.name AS manager, m.salary AS manager_salary
             FROM pg_tsj_employees e
             JOIN pg_tsj_employees m ON e.manager_id = m.id
             WHERE e.name = 'Alice'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['employee']);
        $this->assertEquals(250000.00, (float) $rows[0]['salary']);
        $this->assertSame('VP_Eng', $rows[0]['manager']);
        $this->assertEquals(200000.00, (float) $rows[0]['manager_salary']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_tsj_employees VALUES (8, 'NewHire', 4, 'Engineering', 90000.00)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_tsj_employees");
        $this->assertEquals(8, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_tsj_employees')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
