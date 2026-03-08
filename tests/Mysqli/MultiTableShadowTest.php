<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests cross-table shadow consistency on MySQL via MySQLi: operations spanning
 * multiple shadow tables, subquery interactions, and data flow.
 * @spec SPEC-4.2c
 */
class MultiTableShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_mt_departments (id INT PRIMARY KEY, name VARCHAR(255), budget DECIMAL(12,2))',
            'CREATE TABLE mi_mt_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, salary DECIMAL(10,2))',
            'CREATE TABLE mi_mt_projects (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, lead_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_mt_projects', 'mi_mt_employees', 'mi_mt_departments'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_mt_departments (id, name, budget) VALUES (1, 'Engineering', 500000)");
        $this->mysqli->query("INSERT INTO mi_mt_departments (id, name, budget) VALUES (2, 'Marketing', 200000)");
        $this->mysqli->query("INSERT INTO mi_mt_employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 90000)");
        $this->mysqli->query("INSERT INTO mi_mt_employees (id, name, dept_id, salary) VALUES (2, 'Bob', 2, 60000)");
        $this->mysqli->query("INSERT INTO mi_mt_employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 1, 110000)");
        $this->mysqli->query("INSERT INTO mi_mt_employees (id, name, dept_id, salary) VALUES (4, 'Diana', 2, 75000)");
        $this->mysqli->query("INSERT INTO mi_mt_projects (id, name, dept_id, lead_id) VALUES (1, 'Project Alpha', 1, 1)");
        $this->mysqli->query("INSERT INTO mi_mt_projects (id, name, dept_id, lead_id) VALUES (2, 'Project Beta', 2, 2)");
    }

    public function testJoinAcrossThreeShadowTables(): void
    {
        $result = $this->mysqli->query("
            SELECT e.name AS emp_name, d.name AS dept_name, p.name AS project_name
            FROM mi_mt_employees e
            JOIN mi_mt_departments d ON e.dept_id = d.id
            JOIN mi_mt_projects p ON p.lead_id = e.id
            ORDER BY e.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['emp_name']);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Project Alpha', $rows[0]['project_name']);
    }

    public function testSubqueryInWhere(): void
    {
        $result = $this->mysqli->query("
            SELECT name FROM mi_mt_employees
            WHERE dept_id IN (SELECT id FROM mi_mt_departments WHERE budget > 300000)
            ORDER BY name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testUpdateBasedOnSubquery(): void
    {
        $this->mysqli->query("
            UPDATE mi_mt_employees SET salary = salary * 1.1
            WHERE dept_id IN (SELECT id FROM mi_mt_departments WHERE budget > 300000)
        ");

        $result = $this->mysqli->query('SELECT name, salary FROM mi_mt_employees ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertEquals(99000, (float) $rows[0]['salary']);  // Alice
        $this->assertEquals(60000, (float) $rows[1]['salary']);  // Bob: unchanged
        $this->assertEquals(121000, (float) $rows[2]['salary']); // Charlie
        $this->assertEquals(75000, (float) $rows[3]['salary']);  // Diana: unchanged
    }

    public function testDeleteBasedOnSubquery(): void
    {
        $this->mysqli->query("
            DELETE FROM mi_mt_projects WHERE lead_id IN (
                SELECT id FROM mi_mt_employees WHERE salary < 70000
            )
        ");

        $result = $this->mysqli->query('SELECT * FROM mi_mt_projects ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Project Alpha', $rows[0]['name']);
    }

    public function testSequentialOperationsConsistent(): void
    {
        $this->mysqli->query("INSERT INTO mi_mt_departments (id, name, budget) VALUES (3, 'Research', 400000)");
        $this->mysqli->query("UPDATE mi_mt_employees SET dept_id = 3 WHERE name = 'Charlie'");
        $this->mysqli->query("INSERT INTO mi_mt_projects (id, name, dept_id, lead_id) VALUES (3, 'Project Gamma', 3, 3)");

        $result = $this->mysqli->query("
            SELECT e.name AS emp, d.name AS dept, p.name AS project
            FROM mi_mt_employees e
            JOIN mi_mt_departments d ON e.dept_id = d.id
            LEFT JOIN mi_mt_projects p ON p.lead_id = e.id AND p.dept_id = d.id
            WHERE d.name = 'Research'
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['emp']);
        $this->assertSame('Research', $rows[0]['dept']);
        $this->assertSame('Project Gamma', $rows[0]['project']);
    }

    public function testAllShadowDataIsolated(): void
    {
        $this->mysqli->disableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) as c FROM mi_mt_departments');
        $this->assertSame(0, (int) $result->fetch_assoc()['c']);

        $result = $this->mysqli->query('SELECT COUNT(*) as c FROM mi_mt_employees');
        $this->assertSame(0, (int) $result->fetch_assoc()['c']);

        $result = $this->mysqli->query('SELECT COUNT(*) as c FROM mi_mt_projects');
        $this->assertSame(0, (int) $result->fetch_assoc()['c']);
    }
}
