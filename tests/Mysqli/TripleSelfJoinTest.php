<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests triple and quadruple self-joins through the ZTD CTE rewriter on MySQLi.
 * Simulates an organizational hierarchy with CEO -> VP -> Manager -> IC.
 * @spec SPEC-10.2.99
 */
class TripleSelfJoinTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_tsj_employees (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            manager_id INT,
            department VARCHAR(255),
            salary DECIMAL(10,2)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_tsj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // CEO (level 0)
        $this->mysqli->query("INSERT INTO mi_tsj_employees VALUES (1, 'Alice',   NULL, 'Executive',   200000.00)");
        // VPs (level 1)
        $this->mysqli->query("INSERT INTO mi_tsj_employees VALUES (2, 'Bob',     1,    'Engineering', 150000.00)");
        $this->mysqli->query("INSERT INTO mi_tsj_employees VALUES (3, 'Charlie', 1,    'Sales',       140000.00)");
        // Managers (level 2)
        $this->mysqli->query("INSERT INTO mi_tsj_employees VALUES (4, 'Diana',   2,    'Engineering',  95000.00)");
        $this->mysqli->query("INSERT INTO mi_tsj_employees VALUES (5, 'Eve',     3,    'Sales',        90000.00)");
        // Individual contributors (level 3)
        $this->mysqli->query("INSERT INTO mi_tsj_employees VALUES (6, 'Frank',   4,    'Engineering',  75000.00)");
        $this->mysqli->query("INSERT INTO mi_tsj_employees VALUES (7, 'Grace',   5,    'Sales',        70000.00)");
    }

    /**
     * Triple self-join: employee -> manager -> grandmanager.
     * @spec SPEC-10.2.99
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT e.name AS employee,
                   m.name AS manager,
                   gm.name AS grandmanager
            FROM mi_tsj_employees e
            JOIN mi_tsj_employees m ON e.manager_id = m.id
            JOIN mi_tsj_employees gm ON m.manager_id = gm.id
            ORDER BY e.name
        ");

        // Employees with both manager and grandmanager:
        // Diana -> Bob -> Alice
        // Eve -> Charlie -> Alice
        // Frank -> Diana -> Bob
        // Grace -> Eve -> Charlie
        $this->assertCount(4, $rows);

        $diana = array_values(array_filter($rows, fn($r) => $r['employee'] === 'Diana'));
        $this->assertCount(1, $diana);
        $this->assertSame('Bob', $diana[0]['manager']);
        $this->assertSame('Alice', $diana[0]['grandmanager']);

        $frank = array_values(array_filter($rows, fn($r) => $r['employee'] === 'Frank'));
        $this->assertCount(1, $frank);
        $this->assertSame('Diana', $frank[0]['manager']);
        $this->assertSame('Bob', $frank[0]['grandmanager']);
    }

    /**
     * Quadruple self-join: employee -> manager -> grandmanager -> great-grandmanager.
     * @spec SPEC-10.2.99
     */
    public function testQuadrupleSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT e.name AS employee,
                   m.name AS manager,
                   gm.name AS grandmanager,
                   ggm.name AS great_grandmanager
            FROM mi_tsj_employees e
            JOIN mi_tsj_employees m ON e.manager_id = m.id
            JOIN mi_tsj_employees gm ON m.manager_id = gm.id
            JOIN mi_tsj_employees ggm ON gm.manager_id = ggm.id
            ORDER BY e.name
        ");

        // Frank -> Diana -> Bob -> Alice
        // Grace -> Eve -> Charlie -> Alice
        $this->assertCount(2, $rows);
        $this->assertSame('Frank', $rows[0]['employee']);
        $this->assertSame('Alice', $rows[0]['great_grandmanager']);
        $this->assertSame('Grace', $rows[1]['employee']);
        $this->assertSame('Alice', $rows[1]['great_grandmanager']);
    }

    /**
     * Self-join with aggregate: count of reports at each level under each VP.
     * @spec SPEC-10.2.99
     */
    public function testSelfJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT vp.name AS vp_name,
                   COUNT(DISTINCT mgr.id) AS manager_count,
                   COUNT(DISTINCT ic.id) AS ic_count
            FROM mi_tsj_employees vp
            JOIN mi_tsj_employees mgr ON mgr.manager_id = vp.id
            LEFT JOIN mi_tsj_employees ic ON ic.manager_id = mgr.id
            WHERE vp.manager_id = 1
            GROUP BY vp.id, vp.name
            ORDER BY vp.name
        ");

        // Bob: 1 manager (Diana), 1 IC (Frank)
        // Charlie: 1 manager (Eve), 1 IC (Grace)
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['vp_name']);
        $this->assertEquals(1, (int) $rows[0]['manager_count']);
        $this->assertEquals(1, (int) $rows[0]['ic_count']);
        $this->assertSame('Charlie', $rows[1]['vp_name']);
        $this->assertEquals(1, (int) $rows[1]['manager_count']);
        $this->assertEquals(1, (int) $rows[1]['ic_count']);
    }

    /**
     * Self-join with subquery: find employees earning more than their department average.
     * @spec SPEC-10.2.99
     */
    public function testSelfJoinWithSubquery(): void
    {
        $rows = $this->ztdQuery("
            SELECT e.name, e.department, e.salary
            FROM mi_tsj_employees e
            WHERE e.salary > (
                SELECT AVG(e2.salary)
                FROM mi_tsj_employees e2
                WHERE e2.department = e.department
            )
            ORDER BY e.salary DESC
        ");

        // Engineering: avg = (150000+95000+75000)/3 = 106666.67 => Bob(150k) above
        // Sales: avg = (140000+90000+70000)/3 = 100000 => Charlie(140k) above
        // Executive: avg = 200000 => nobody above
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    /**
     * Self-join correctly reflects data after a mutation.
     * @spec SPEC-10.2.99
     */
    public function testSelfJoinAfterMutation(): void
    {
        // Promote Frank to report directly to Bob (skip Diana)
        $this->mysqli->query("UPDATE mi_tsj_employees SET manager_id = 2 WHERE id = 6");

        $rows = $this->ztdQuery("
            SELECT e.name AS employee, m.name AS manager
            FROM mi_tsj_employees e
            JOIN mi_tsj_employees m ON e.manager_id = m.id
            WHERE e.id = 6
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('Frank', $rows[0]['employee']);
        $this->assertSame('Bob', $rows[0]['manager']);

        // Bob now has 2 direct reports (Diana, Frank)
        $rows2 = $this->ztdQuery("
            SELECT COUNT(*) AS cnt FROM mi_tsj_employees WHERE manager_id = 2
        ");
        $this->assertEquals(2, (int) $rows2[0]['cnt']);
    }

    /**
     * Physical table remains empty — all mutations are in ZTD shadow store.
     * @spec SPEC-10.2.99
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_tsj_employees SET salary = 999999.99 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT salary FROM mi_tsj_employees WHERE id = 1");
        $this->assertEqualsWithDelta(999999.99, (float) $rows[0]['salary'], 0.01);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_tsj_employees');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
