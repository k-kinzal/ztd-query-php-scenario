<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests triple and quadruple self-joins through the ZTD CTE rewriter on MySQL via PDO.
 * Covers employee-manager hierarchies with 3+ aliases, aggregate over self-join,
 * self-join with subquery, and reads after mutation.
 * @spec SPEC-10.2.99
 */
class MysqlTripleSelfJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_tsj_employees (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            manager_id INT,
            department VARCHAR(255),
            salary DECIMAL(10,2)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_tsj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (1, 'CEO',     NULL, 'Executive', 300000.00)");
        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (2, 'VP Eng',  1,    'Executive', 250000.00)");
        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (3, 'VP Sales',1,    'Executive', 240000.00)");
        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (4, 'Alice',   2,    'Engineering', 120000.00)");
        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (5, 'Bob',     2,    'Engineering', 110000.00)");
        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (6, 'Charlie', 4,    'Engineering', 90000.00)");
        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (7, 'Diana',   3,    'Sales',       100000.00)");
        $this->pdo->exec("INSERT INTO mp_tsj_employees VALUES (8, 'Eve',     4,    'Engineering', 85000.00)");
    }

    /**
     * @spec SPEC-10.2.99
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT e.name AS employee,
                   m.name AS manager,
                   gm.name AS grand_manager
            FROM mp_tsj_employees e
            JOIN mp_tsj_employees m ON e.manager_id = m.id
            JOIN mp_tsj_employees gm ON m.manager_id = gm.id
            ORDER BY e.name
        ");

        // Employees with both manager and grand-manager:
        // Alice -> VP Eng -> CEO
        // Bob -> VP Eng -> CEO
        // Charlie -> Alice -> VP Eng
        // Diana -> VP Sales -> CEO
        // Eve -> Alice -> VP Eng
        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['employee']);
        $this->assertSame('VP Eng', $rows[0]['manager']);
        $this->assertSame('CEO', $rows[0]['grand_manager']);
        $this->assertSame('Charlie', $rows[2]['employee']);
        $this->assertSame('Alice', $rows[2]['manager']);
        $this->assertSame('VP Eng', $rows[2]['grand_manager']);
    }

    /**
     * @spec SPEC-10.2.99
     */
    public function testQuadrupleSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT e.name AS employee,
                   m.name AS manager,
                   gm.name AS grand_manager,
                   ggm.name AS great_grand_manager
            FROM mp_tsj_employees e
            JOIN mp_tsj_employees m ON e.manager_id = m.id
            JOIN mp_tsj_employees gm ON m.manager_id = gm.id
            JOIN mp_tsj_employees ggm ON gm.manager_id = ggm.id
            ORDER BY e.name
        ");

        // Employees with 3 levels above:
        // Charlie -> Alice -> VP Eng -> CEO
        // Eve -> Alice -> VP Eng -> CEO
        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['employee']);
        $this->assertSame('Alice', $rows[0]['manager']);
        $this->assertSame('VP Eng', $rows[0]['grand_manager']);
        $this->assertSame('CEO', $rows[0]['great_grand_manager']);
        $this->assertSame('Eve', $rows[1]['employee']);
        $this->assertSame('CEO', $rows[1]['great_grand_manager']);
    }

    /**
     * @spec SPEC-10.2.99
     */
    public function testSelfJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT m.name AS manager,
                   COUNT(e.id) AS direct_reports,
                   AVG(e.salary) AS avg_report_salary
            FROM mp_tsj_employees e
            JOIN mp_tsj_employees m ON e.manager_id = m.id
            GROUP BY m.id, m.name
            HAVING COUNT(e.id) > 1
            ORDER BY direct_reports DESC
        ");

        // VP Eng manages Alice, Bob (2 reports)
        // CEO manages VP Eng, VP Sales (2 reports)
        // Alice manages Charlie, Eve (2 reports)
        $this->assertCount(3, $rows);
        $managers = array_column($rows, 'manager');
        $this->assertContains('CEO', $managers);
        $this->assertContains('VP Eng', $managers);
        $this->assertContains('Alice', $managers);
    }

    /**
     * @spec SPEC-10.2.99
     */
    public function testSelfJoinWithSubquery(): void
    {
        $rows = $this->ztdQuery("
            SELECT e.name, e.salary
            FROM mp_tsj_employees e
            WHERE e.salary > (
                SELECT AVG(sub.salary)
                FROM mp_tsj_employees sub
                WHERE sub.manager_id = e.manager_id
                  AND sub.id != e.id
            )
            ORDER BY e.name
        ");

        // Among siblings (same manager):
        // Under CEO: VP Eng (250k) > VP Sales (240k) avg = 240k, VP Sales < VP Eng avg = 250k
        // Under VP Eng: Alice (120k) > Bob (110k) avg = 110k
        // Under Alice: Charlie (90k) > Eve (85k) avg = 85k
        $this->assertGreaterThanOrEqual(1, count($rows));
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
    }

    /**
     * @spec SPEC-10.2.99
     */
    public function testSelfJoinAfterMutation(): void
    {
        $this->ztdExec("UPDATE mp_tsj_employees SET manager_id = 3 WHERE id = 6");

        // Charlie now reports to VP Sales instead of Alice
        $rows = $this->ztdQuery("
            SELECT e.name AS employee,
                   m.name AS manager,
                   gm.name AS grand_manager
            FROM mp_tsj_employees e
            JOIN mp_tsj_employees m ON e.manager_id = m.id
            JOIN mp_tsj_employees gm ON m.manager_id = gm.id
            WHERE e.id = 6
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['employee']);
        $this->assertSame('VP Sales', $rows[0]['manager']);
        $this->assertSame('CEO', $rows[0]['grand_manager']);
    }

    /**
     * @spec SPEC-10.2.99
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT INTO mp_tsj_employees VALUES (9, 'Frank', 5, 'Engineering', 80000.00)");
        $this->ztdExec("UPDATE mp_tsj_employees SET salary = 999999.99 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_tsj_employees");
        $this->assertSame(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_tsj_employees')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
