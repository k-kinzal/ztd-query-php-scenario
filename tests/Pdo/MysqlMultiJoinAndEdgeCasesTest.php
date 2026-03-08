<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multi-table JOINs (4+ tables), INSERT without column list,
 * SQL comments, and edge cases on MySQL PDO.
 * @spec SPEC-3.3
 */
class MysqlMultiJoinAndEdgeCasesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_mj_departments (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE mysql_mj_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, manager_id INT)',
            'CREATE TABLE mysql_mj_projects (id INT PRIMARY KEY, title VARCHAR(255), dept_id INT)',
            'CREATE TABLE mysql_mj_assignments (employee_id INT, project_id INT, hours INT)',
            'CREATE TABLE mysql_mj_reviews (id INT PRIMARY KEY, employee_id INT, score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_mj_assignments', 'mysql_mj_reviews', 'mysql_mj_projects', 'mysql_mj_employees', 'mysql_mj_departments'];
    }


    public function testFiveTableJoin(): void
    {
        $this->pdo->exec("INSERT INTO mysql_mj_departments (id, name) VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_mj_employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO mysql_mj_projects (id, title, dept_id) VALUES (1, 'API', 1)");
        $this->pdo->exec("INSERT INTO mysql_mj_assignments (employee_id, project_id, hours) VALUES (1, 1, 40)");
        $this->pdo->exec("INSERT INTO mysql_mj_reviews (id, employee_id, score) VALUES (1, 1, 95)");

        $stmt = $this->pdo->query("
            SELECT e.name, d.name AS dept, p.title, a.hours, r.score
            FROM mysql_mj_employees e
            JOIN mysql_mj_departments d ON e.dept_id = d.id
            JOIN mysql_mj_assignments a ON a.employee_id = e.id
            JOIN mysql_mj_projects p ON a.project_id = p.id
            JOIN mysql_mj_reviews r ON r.employee_id = e.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Engineering', $rows[0]['dept']);
        $this->assertSame(95, (int) $rows[0]['score']);
    }

    public function testInsertWithoutColumnList(): void
    {
        $this->pdo->exec("INSERT INTO mysql_mj_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_mj_departments VALUES (2, 'Marketing')");

        $stmt = $this->pdo->query("SELECT name FROM mysql_mj_departments ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    public function testNotEqualOperator(): void
    {
        $this->pdo->exec("INSERT INTO mysql_mj_employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO mysql_mj_employees (id, name, dept_id, manager_id) VALUES (2, 'Bob', 2, 1)");

        $stmt = $this->pdo->query("SELECT name FROM mysql_mj_employees WHERE dept_id != 1");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testSqlWithComments(): void
    {
        $this->pdo->exec("INSERT INTO mysql_mj_departments (id, name) VALUES (1, 'Engineering')");

        $stmt = $this->pdo->query("
            SELECT /* block comment */ name -- inline comment
            FROM mysql_mj_departments
            WHERE id = 1
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Engineering', $row['name']);
    }
}
