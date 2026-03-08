<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests complex UPDATE patterns on MySQLi: CASE in SET, arithmetic expressions,
 * multiple sequential mutations, UPDATE-DELETE-count chain.
 * @spec pending
 */
class ComplexUpdatePatternsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_cup_employees (id INT PRIMARY KEY, name VARCHAR(50), department VARCHAR(30), salary DECIMAL(10,2), grade VARCHAR(5), active TINYINT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_cup_employees'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cup_employees VALUES (1, 'Alice', 'Engineering', 80000, 'B', 1)");
        $this->mysqli->query("INSERT INTO mi_cup_employees VALUES (2, 'Bob', 'Engineering', 90000, 'A', 1)");
        $this->mysqli->query("INSERT INTO mi_cup_employees VALUES (3, 'Charlie', 'Sales', 60000, 'C', 1)");
        $this->mysqli->query("INSERT INTO mi_cup_employees VALUES (4, 'Diana', 'Sales', 70000, 'B', 0)");
    }

    public function testUpdateWithCaseInSet(): void
    {
        $this->mysqli->query("
            UPDATE mi_cup_employees SET grade = CASE
                WHEN salary >= 90000 THEN 'S'
                WHEN salary >= 75000 THEN 'A'
                ELSE 'B'
            END
        ");

        $result = $this->mysqli->query("SELECT name, grade FROM mi_cup_employees ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('A', $rows[0]['grade']);
        $this->assertSame('S', $rows[1]['grade']);
    }

    public function testUpdateWithConditionalArithmetic(): void
    {
        $this->mysqli->query("
            UPDATE mi_cup_employees SET salary = CASE
                WHEN grade = 'A' THEN salary * 1.15
                WHEN grade = 'B' THEN salary * 1.10
                ELSE salary * 1.05
            END
            WHERE active = 1
        ");

        $result = $this->mysqli->query("SELECT name, salary FROM mi_cup_employees WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(88000.0, (float) $row['salary'], 0.01);
    }

    public function testUpdateThenDeleteThenCount(): void
    {
        $this->mysqli->query("UPDATE mi_cup_employees SET active = 0 WHERE department = 'Sales'");
        $this->mysqli->query("DELETE FROM mi_cup_employees WHERE active = 0");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_cup_employees");
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testMultipleSequentialUpdates(): void
    {
        $this->mysqli->query("UPDATE mi_cup_employees SET salary = salary * 1.05");
        $this->mysqli->query("UPDATE mi_cup_employees SET grade = 'S' WHERE salary > 80000 AND active = 1");

        $result = $this->mysqli->query("SELECT grade FROM mi_cup_employees WHERE id = 2");
        $row = $result->fetch_assoc();
        $this->assertSame('S', $row['grade']);
    }
}
