<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests NATURAL JOIN through the CTE rewriter on MySQL.
 * Cross-platform verification of SqliteNaturalJoinTest.
 *
 * SQL patterns exercised: NATURAL JOIN, NATURAL LEFT JOIN, NATURAL JOIN
 * with shadow data, NATURAL JOIN with aggregate.
 * @spec SPEC-3.3
 */
class MysqlNaturalJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_nj_departments (
                dept_id INT PRIMARY KEY,
                dept_name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_nj_employees (
                emp_id INT PRIMARY KEY,
                emp_name VARCHAR(100) NOT NULL,
                dept_id INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_nj_employees', 'my_nj_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_nj_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO my_nj_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO my_nj_departments VALUES (3, 'Sales')");

        $this->pdo->exec("INSERT INTO my_nj_employees VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO my_nj_employees VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO my_nj_employees VALUES (3, 'Carol', 2)");
    }

    /**
     * Basic NATURAL JOIN — should match on dept_id column.
     */
    public function testBasicNaturalJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT emp_name, dept_name
             FROM my_nj_employees NATURAL JOIN my_nj_departments
             ORDER BY emp_name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['emp_name']);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
    }

    /**
     * NATURAL LEFT JOIN — departments with no employees.
     */
    public function testNaturalLeftJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT dept_name, emp_name
             FROM my_nj_departments NATURAL LEFT JOIN my_nj_employees
             ORDER BY dept_name"
        );

        $this->assertCount(4, $rows);
        $salesRow = array_values(array_filter($rows, fn($r) => $r['dept_name'] === 'Sales'));
        $this->assertCount(1, $salesRow);
        $this->assertNull($salesRow[0]['emp_name']);
    }

    /**
     * NATURAL JOIN after shadow INSERT.
     */
    public function testNaturalJoinAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO my_nj_employees VALUES (4, 'Diana', 3)");

        $rows = $this->ztdQuery(
            "SELECT emp_name, dept_name
             FROM my_nj_employees NATURAL JOIN my_nj_departments
             WHERE dept_name = 'Sales'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Diana', $rows[0]['emp_name']);
    }

    /**
     * NATURAL JOIN with aggregate.
     */
    public function testNaturalJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT dept_name, COUNT(emp_id) AS cnt
             FROM my_nj_departments NATURAL LEFT JOIN my_nj_employees
             GROUP BY dept_name
             ORDER BY dept_name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->assertSame('Sales', $rows[2]['dept_name']);
        $this->assertEquals(0, (int) $rows[2]['cnt']);
    }
}
