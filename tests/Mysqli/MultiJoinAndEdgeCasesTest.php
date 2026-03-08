<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multi-table JOINs (4+ tables), INSERT without column list,
 * SQL comments, and edge cases on MySQLi.
 * @spec pending
 */
class MultiJoinAndEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_mj_departments (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE mi_mj_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, manager_id INT)',
            'CREATE TABLE mi_mj_projects (id INT PRIMARY KEY, title VARCHAR(255), dept_id INT)',
            'CREATE TABLE mi_mj_assignments (employee_id INT, project_id INT, hours INT)',
            'CREATE TABLE mi_mj_reviews (id INT PRIMARY KEY, employee_id INT, score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_mj_assignments', 'mi_mj_reviews', 'mi_mj_projects', 'mi_mj_employees', 'mi_mj_departments'];
    }


    public function testFiveTableJoin(): void
    {
        $this->mysqli->query("INSERT INTO mi_mj_departments (id, name) VALUES (1, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_mj_employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->mysqli->query("INSERT INTO mi_mj_projects (id, title, dept_id) VALUES (1, 'API', 1)");
        $this->mysqli->query("INSERT INTO mi_mj_assignments (employee_id, project_id, hours) VALUES (1, 1, 40)");
        $this->mysqli->query("INSERT INTO mi_mj_reviews (id, employee_id, score) VALUES (1, 1, 95)");

        $result = $this->mysqli->query("
            SELECT e.name, d.name AS dept, p.title, a.hours, r.score
            FROM mi_mj_employees e
            JOIN mi_mj_departments d ON e.dept_id = d.id
            JOIN mi_mj_assignments a ON a.employee_id = e.id
            JOIN mi_mj_projects p ON a.project_id = p.id
            JOIN mi_mj_reviews r ON r.employee_id = e.id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(95, (int) $rows[0]['score']);
    }

    public function testInsertWithoutColumnList(): void
    {
        $this->mysqli->query("INSERT INTO mi_mj_departments VALUES (1, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_mj_departments VALUES (2, 'Marketing')");

        $result = $this->mysqli->query("SELECT name FROM mi_mj_departments ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    public function testSqlWithComments(): void
    {
        $this->mysqli->query("INSERT INTO mi_mj_departments (id, name) VALUES (1, 'Engineering')");

        $result = $this->mysqli->query("
            SELECT /* block comment */ name -- inline comment
            FROM mi_mj_departments
            WHERE id = 1
        ");
        $row = $result->fetch_assoc();
        $this->assertSame('Engineering', $row['name']);
    }
}
