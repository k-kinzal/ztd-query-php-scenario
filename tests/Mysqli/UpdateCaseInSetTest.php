<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET col = CASE ... END through MySQLi CTE shadow store.
 *
 * Control test for PostgresUpdateCaseInSetTest. MySQLi uses the same
 * phpMyAdmin SqlParser as MySQL-PDO, so CASE in SET should work.
 */
class UpdateCaseInSetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ucs_employees (
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
        return ['mi_ucs_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_ucs_employees (id, name, department, salary, grade) VALUES (1, 'Alice', 'Engineering', 90000, 'B')");
        $this->ztdExec("INSERT INTO mi_ucs_employees (id, name, department, salary, grade) VALUES (2, 'Bob', 'Marketing', 60000, 'C')");
        $this->ztdExec("INSERT INTO mi_ucs_employees (id, name, department, salary, grade) VALUES (3, 'Carol', 'Engineering', 120000, 'A')");
        $this->ztdExec("INSERT INTO mi_ucs_employees (id, name, department, salary, grade) VALUES (4, 'Dave', 'Sales', 45000, 'D')");
    }

    /**
     * UPDATE SET with CASE expression (no prepared params).
     */
    public function testUpdateSetSimpleCase(): void
    {
        $this->ztdExec(
            "UPDATE mi_ucs_employees SET grade = CASE
                WHEN salary >= 100000 THEN 'A'
                WHEN salary >= 75000 THEN 'B'
                WHEN salary >= 50000 THEN 'C'
                ELSE 'D'
             END"
        );

        $rows = $this->ztdQuery("SELECT name, grade FROM mi_ucs_employees ORDER BY name");
        $this->assertCount(4, $rows);
        $this->assertEquals('B', $rows[0]['grade']); // Alice
        $this->assertEquals('C', $rows[1]['grade']); // Bob
        $this->assertEquals('A', $rows[2]['grade']); // Carol
        $this->assertEquals('D', $rows[3]['grade']); // Dave
    }

    /**
     * UPDATE SET CASE with prepared statement — control test.
     */
    public function testUpdateSetCasePrepared(): void
    {
        $stmt = $this->mysqli->prepare(
            "UPDATE mi_ucs_employees SET salary = CASE
                WHEN salary >= ? THEN salary * 1.10
                ELSE salary * 1.05
             END
             WHERE department = ?"
        );
        $stmt->bind_param('ds', $threshold, $dept);
        $threshold = 80000.0;
        $dept = 'Engineering';
        $stmt->execute();

        $rows = $this->ztdQuery("SELECT name, salary FROM mi_ucs_employees WHERE department = 'Engineering' ORDER BY name");
        $this->assertEquals(99000, (float) $rows[0]['salary']); // Alice
        $this->assertEquals(132000, (float) $rows[1]['salary']); // Carol
    }

    /**
     * UPDATE SET with nested CASE.
     */
    public function testUpdateSetNestedCase(): void
    {
        $this->ztdExec(
            "UPDATE mi_ucs_employees SET grade = CASE
                WHEN department = 'Engineering' THEN
                    CASE WHEN salary >= 100000 THEN 'S' ELSE 'A' END
                WHEN department = 'Marketing' THEN 'B'
                ELSE 'C'
             END"
        );

        $rows = $this->ztdQuery("SELECT name, grade FROM mi_ucs_employees ORDER BY name");
        $this->assertEquals('A', $rows[0]['grade']); // Alice
        $this->assertEquals('B', $rows[1]['grade']); // Bob
        $this->assertEquals('S', $rows[2]['grade']); // Carol
        $this->assertEquals('C', $rows[3]['grade']); // Dave
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ucs_employees');
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }
}
