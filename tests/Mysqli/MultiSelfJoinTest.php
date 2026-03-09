<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multiple self-joins through MySQLi CTE shadow store.
 *
 * @spec SPEC-3.2
 */
class MultiSelfJoinTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_msj_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                manager_id INT,
                department VARCHAR(50) NOT NULL,
                salary DECIMAL(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_msj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_msj_employees VALUES (1, 'CEO', NULL, 'Executive', 200000)");
        $this->mysqli->query("INSERT INTO mi_msj_employees VALUES (2, 'VP Eng', 1, 'Engineering', 150000)");
        $this->mysqli->query("INSERT INTO mi_msj_employees VALUES (3, 'VP Sales', 1, 'Sales', 140000)");
        $this->mysqli->query("INSERT INTO mi_msj_employees VALUES (4, 'Dev Lead', 2, 'Engineering', 120000)");
        $this->mysqli->query("INSERT INTO mi_msj_employees VALUES (5, 'Sr Dev', 4, 'Engineering', 100000)");
        $this->mysqli->query("INSERT INTO mi_msj_employees VALUES (6, 'Jr Dev', 4, 'Engineering', 70000)");
    }

    public function testEmployeeWithManager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager
             FROM mi_msj_employees e
             LEFT JOIN mi_msj_employees m ON m.id = e.manager_id
             ORDER BY e.id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
    }

    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m1.name AS l1, m2.name AS l2, m3.name AS l3
             FROM mi_msj_employees e
             LEFT JOIN mi_msj_employees m1 ON m1.id = e.manager_id
             LEFT JOIN mi_msj_employees m2 ON m2.id = m1.manager_id
             LEFT JOIN mi_msj_employees m3 ON m3.id = m2.manager_id
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
             FROM mi_msj_employees m
             LEFT JOIN mi_msj_employees e ON e.manager_id = m.id
             GROUP BY m.id, m.name
             HAVING COUNT(e.id) > 0
             ORDER BY direct_reports DESC"
        );

        $this->assertGreaterThanOrEqual(2, count($rows));
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $raw = new \mysqli(
            \Tests\Support\MySQLContainer::getHost(),
            'root', 'root', 'test',
            \Tests\Support\MySQLContainer::getPort(),
        );
        $result = $raw->query("SELECT COUNT(*) AS cnt FROM mi_msj_employees");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
        $raw->close();
    }
}
