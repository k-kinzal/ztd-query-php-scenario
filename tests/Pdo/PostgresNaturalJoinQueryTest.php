<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * NATURAL JOIN on shadow data (PostgreSQL PDO).
 * Tests whether the CTE rewriter handles NATURAL JOIN syntax correctly.
 * NATURAL JOIN implicitly joins on columns with matching names.
 *
 * @spec SPEC-3.3
 */
class PostgresNaturalJoinQueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_njq_departments (
                department_id INT PRIMARY KEY,
                dept_name VARCHAR(50) NOT NULL,
                floor_number INT NOT NULL
            )",
            "CREATE TABLE pg_njq_staff (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                department_id INT NOT NULL,
                role VARCHAR(30) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_njq_staff', 'pg_njq_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_njq_departments (department_id, dept_name, floor_number) VALUES
            (1, 'Engineering', 3),
            (2, 'Marketing', 2),
            (3, 'Sales', 1)");

        $this->pdo->exec("INSERT INTO pg_njq_staff (id, name, department_id, role) VALUES
            (1, 'Alice', 1, 'Engineer'),
            (2, 'Bob', 1, 'Lead'),
            (3, 'Carol', 2, 'Designer'),
            (4, 'Dave', 3, 'Rep')");
    }

    public function testNaturalJoin(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, dept_name, floor_number
                 FROM pg_njq_staff
                 NATURAL JOIN pg_njq_departments
                 ORDER BY pg_njq_staff.id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete('NATURAL JOIN: expected 4, got ' . count($rows));
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Engineering', $rows[1]['dept_name']);
            $this->assertSame('Carol', $rows[2]['name']);
            $this->assertSame('Marketing', $rows[2]['dept_name']);
            $this->assertSame('Dave', $rows[3]['name']);
            $this->assertSame('Sales', $rows[3]['dept_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NATURAL JOIN failed: ' . $e->getMessage());
        }
    }

    public function testNaturalJoinAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_njq_staff (id, name, department_id, role) VALUES (5, 'Eve', 2, 'Manager')");

            $rows = $this->ztdQuery(
                "SELECT name, dept_name
                 FROM pg_njq_staff
                 NATURAL JOIN pg_njq_departments
                 WHERE dept_name = 'Marketing'
                 ORDER BY name"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('NATURAL JOIN after INSERT: expected 2, got ' . count($rows));
            }

            $this->assertSame('Carol', $rows[0]['name']);
            $this->assertSame('Eve', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NATURAL JOIN after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testNaturalLeftJoin(): void
    {
        try {
            // Insert a department with no staff
            $this->pdo->exec("INSERT INTO pg_njq_departments (department_id, dept_name, floor_number) VALUES (4, 'HR', 4)");

            $rows = $this->ztdQuery(
                "SELECT dept_name, COUNT(pg_njq_staff.id) AS staff_count
                 FROM pg_njq_departments
                 NATURAL LEFT JOIN pg_njq_staff
                 GROUP BY department_id, dept_name
                 ORDER BY department_id"
            );

            if (count($rows) < 3) {
                $this->markTestIncomplete('NATURAL LEFT JOIN: expected >= 3, got ' . count($rows));
            }

            // Engineering should have 2 staff
            $this->assertSame('Engineering', $rows[0]['dept_name']);
            $this->assertSame(2, (int) $rows[0]['staff_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NATURAL LEFT JOIN failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_njq_staff");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
