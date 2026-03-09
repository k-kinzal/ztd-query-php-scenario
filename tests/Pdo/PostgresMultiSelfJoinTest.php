<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests multiple self-joins through PostgreSQL CTE shadow store.
 *
 * @spec SPEC-3.2
 */
class PostgresMultiSelfJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_msj_employees (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                manager_id INT,
                department TEXT NOT NULL,
                salary NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_msj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_msj_employees VALUES (1, 'CEO', NULL, 'Executive', 200000)");
        $this->pdo->exec("INSERT INTO pg_msj_employees VALUES (2, 'VP Eng', 1, 'Engineering', 150000)");
        $this->pdo->exec("INSERT INTO pg_msj_employees VALUES (3, 'VP Sales', 1, 'Sales', 140000)");
        $this->pdo->exec("INSERT INTO pg_msj_employees VALUES (4, 'Dev Lead', 2, 'Engineering', 120000)");
        $this->pdo->exec("INSERT INTO pg_msj_employees VALUES (5, 'Sr Dev', 4, 'Engineering', 100000)");
        $this->pdo->exec("INSERT INTO pg_msj_employees VALUES (6, 'Jr Dev', 4, 'Engineering', 70000)");
    }

    public function testEmployeeWithManager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager
             FROM pg_msj_employees e
             LEFT JOIN pg_msj_employees m ON m.id = e.manager_id
             ORDER BY e.id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
        $this->assertSame('VP Eng', $rows[1]['employee']);
        $this->assertSame('CEO', $rows[1]['manager']);
    }

    public function testTwoLevelSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager, gm.name AS grand_manager
             FROM pg_msj_employees e
             LEFT JOIN pg_msj_employees m ON m.id = e.manager_id
             LEFT JOIN pg_msj_employees gm ON gm.id = m.manager_id
             WHERE gm.id IS NOT NULL
             ORDER BY e.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Dev Lead', $rows[0]['employee']);
        $this->assertSame('CEO', $rows[0]['grand_manager']);
    }

    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m1.name AS l1, m2.name AS l2, m3.name AS l3
             FROM pg_msj_employees e
             LEFT JOIN pg_msj_employees m1 ON m1.id = e.manager_id
             LEFT JOIN pg_msj_employees m2 ON m2.id = m1.manager_id
             LEFT JOIN pg_msj_employees m3 ON m3.id = m2.manager_id
             WHERE m3.id IS NOT NULL
             ORDER BY e.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Jr Dev', $rows[0]['employee']);
        $this->assertSame('CEO', $rows[0]['l3']);
    }

    public function testSelfJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name AS manager, COUNT(e.id) AS direct_reports
             FROM pg_msj_employees m
             LEFT JOIN pg_msj_employees e ON e.manager_id = m.id
             GROUP BY m.id, m.name
             HAVING COUNT(e.id) > 0
             ORDER BY direct_reports DESC, m.name"
        );

        $this->assertGreaterThanOrEqual(2, count($rows));
    }

    public function testPreparedSelfJoin(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name AS employee, m.name AS manager
             FROM pg_msj_employees e
             LEFT JOIN pg_msj_employees m ON m.id = e.manager_id
             WHERE e.department = ?
             ORDER BY e.salary DESC",
            ['Engineering']
        );

        $this->assertCount(4, $rows);
        $this->assertSame('VP Eng', $rows[0]['employee']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) FROM pg_msj_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['count']);
    }
}
