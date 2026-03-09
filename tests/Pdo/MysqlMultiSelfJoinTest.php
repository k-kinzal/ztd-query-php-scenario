<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multiple self-joins through MySQL PDO CTE shadow store.
 *
 * @spec SPEC-3.2
 */
class MysqlMultiSelfJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_msj_employees (
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
        return ['my_msj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_msj_employees VALUES (1, 'CEO', NULL, 'Executive', 200000)");
        $this->pdo->exec("INSERT INTO my_msj_employees VALUES (2, 'VP Eng', 1, 'Engineering', 150000)");
        $this->pdo->exec("INSERT INTO my_msj_employees VALUES (3, 'VP Sales', 1, 'Sales', 140000)");
        $this->pdo->exec("INSERT INTO my_msj_employees VALUES (4, 'Dev Lead', 2, 'Engineering', 120000)");
        $this->pdo->exec("INSERT INTO my_msj_employees VALUES (5, 'Sr Dev', 4, 'Engineering', 100000)");
        $this->pdo->exec("INSERT INTO my_msj_employees VALUES (6, 'Jr Dev', 4, 'Engineering', 70000)");
    }

    public function testEmployeeWithManager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager
             FROM my_msj_employees e
             LEFT JOIN my_msj_employees m ON m.id = e.manager_id
             ORDER BY e.id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
    }

    public function testTwoLevelSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS employee, m.name AS manager, gm.name AS grand_manager
             FROM my_msj_employees e
             LEFT JOIN my_msj_employees m ON m.id = e.manager_id
             LEFT JOIN my_msj_employees gm ON gm.id = m.manager_id
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
             FROM my_msj_employees e
             LEFT JOIN my_msj_employees m1 ON m1.id = e.manager_id
             LEFT JOIN my_msj_employees m2 ON m2.id = m1.manager_id
             LEFT JOIN my_msj_employees m3 ON m3.id = m2.manager_id
             WHERE m3.id IS NOT NULL
             ORDER BY e.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Jr Dev', $rows[0]['employee']);
        $this->assertSame('CEO', $rows[0]['l3']);
    }

    public function testSelfJoinPairs(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e1.name AS emp1, e2.name AS emp2
             FROM my_msj_employees e1
             JOIN my_msj_employees e2 ON e1.department = e2.department AND e1.id < e2.id
             WHERE e1.department = 'Engineering'
             ORDER BY e1.name, e2.name"
        );

        $this->assertCount(6, $rows);
    }

    public function testPreparedSelfJoin(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name AS employee, m.name AS manager
             FROM my_msj_employees e
             LEFT JOIN my_msj_employees m ON m.id = e.manager_id
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
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_msj_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
