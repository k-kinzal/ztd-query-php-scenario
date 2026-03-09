<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE SET col = CASE ... END through MySQL-PDO CTE shadow store.
 *
 * Control test for PostgresUpdateCaseInSetTest. Verifies whether CASE
 * expressions in SET clauses work on MySQL (which uses phpMyAdmin SqlParser
 * instead of regex).
 */
class MysqlUpdateCaseInSetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ucs_employees (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(50) NOT NULL,
                salary DECIMAL(10,2) NOT NULL,
                grade VARCHAR(10) NOT NULL DEFAULT \'C\'
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ucs_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ucs_employees (id, name, department, salary, grade) VALUES (1, 'Alice', 'Engineering', 90000, 'B')");
        $this->pdo->exec("INSERT INTO mp_ucs_employees (id, name, department, salary, grade) VALUES (2, 'Bob', 'Marketing', 60000, 'C')");
        $this->pdo->exec("INSERT INTO mp_ucs_employees (id, name, department, salary, grade) VALUES (3, 'Carol', 'Engineering', 120000, 'A')");
        $this->pdo->exec("INSERT INTO mp_ucs_employees (id, name, department, salary, grade) VALUES (4, 'Dave', 'Sales', 45000, 'D')");
    }

    /**
     * UPDATE SET with simple CASE expression (no prepared params).
     */
    public function testUpdateSetSimpleCase(): void
    {
        $this->pdo->exec(
            "UPDATE mp_ucs_employees SET grade = CASE
                WHEN salary >= 100000 THEN 'A'
                WHEN salary >= 75000 THEN 'B'
                WHEN salary >= 50000 THEN 'C'
                ELSE 'D'
             END"
        );

        $rows = $this->ztdQuery("SELECT name, grade FROM mp_ucs_employees ORDER BY name");
        $this->assertCount(4, $rows);
        $this->assertEquals('B', $rows[0]['grade']); // Alice
        $this->assertEquals('C', $rows[1]['grade']); // Bob
        $this->assertEquals('A', $rows[2]['grade']); // Carol
        $this->assertEquals('D', $rows[3]['grade']); // Dave
    }

    /**
     * UPDATE SET with CASE and WHERE clause.
     */
    public function testUpdateSetCaseWithWhere(): void
    {
        $this->pdo->exec(
            "UPDATE mp_ucs_employees SET salary = CASE
                WHEN department = 'Engineering' THEN salary * 1.10
                ELSE salary * 1.05
             END
             WHERE grade IN ('B', 'C')"
        );

        $rows = $this->ztdQuery("SELECT name, salary FROM mp_ucs_employees ORDER BY name");
        $this->assertEquals(99000, (float) $rows[0]['salary']); // Alice
        $this->assertEquals(63000, (float) $rows[1]['salary']); // Bob
        $this->assertEquals(120000, (float) $rows[2]['salary']); // Carol: unchanged
        $this->assertEquals(45000, (float) $rows[3]['salary']); // Dave: unchanged
    }

    /**
     * UPDATE SET with nested CASE.
     */
    public function testUpdateSetNestedCase(): void
    {
        $this->pdo->exec(
            "UPDATE mp_ucs_employees SET grade = CASE
                WHEN department = 'Engineering' THEN
                    CASE WHEN salary >= 100000 THEN 'S' ELSE 'A' END
                WHEN department = 'Marketing' THEN 'B'
                ELSE 'C'
             END"
        );

        $rows = $this->ztdQuery("SELECT name, grade FROM mp_ucs_employees ORDER BY name");
        $this->assertEquals('A', $rows[0]['grade']); // Alice
        $this->assertEquals('B', $rows[1]['grade']); // Bob
        $this->assertEquals('S', $rows[2]['grade']); // Carol
        $this->assertEquals('C', $rows[3]['grade']); // Dave
    }

    /**
     * UPDATE SET CASE with prepared statement — control test.
     */
    public function testUpdateSetCasePrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "UPDATE mp_ucs_employees SET salary = CASE
                WHEN salary >= ? THEN salary * 1.10
                ELSE salary * 1.05
             END
             WHERE department = ?"
        );
        $stmt->execute([80000, 'Engineering']);

        $rows = $this->ztdQuery("SELECT name, salary FROM mp_ucs_employees WHERE department = 'Engineering' ORDER BY name");
        $this->assertEquals(99000, (float) $rows[0]['salary']); // Alice
        $this->assertEquals(132000, (float) $rows[1]['salary']); // Carol
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM mp_ucs_employees')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
