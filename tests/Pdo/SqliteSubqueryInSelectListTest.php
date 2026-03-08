<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subqueries in various positions within SELECT on SQLite.
 *
 * Verifies scalar subqueries in SELECT list, EXISTS in WHERE,
 * subqueries in CASE WHEN, and nested subqueries all work
 * correctly through CTE rewriting.
 * @spec SPEC-3.3
 */
class SqliteSubqueryInSelectListTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ssl_dept (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE ssl_emp (id INT PRIMARY KEY, name VARCHAR(50), dept_id INT, salary DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ssl_dept', 'ssl_emp'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ssl_dept VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO ssl_dept VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO ssl_dept VALUES (3, 'HR')");
        $this->pdo->exec("INSERT INTO ssl_emp VALUES (1, 'Alice', 1, 120000)");
        $this->pdo->exec("INSERT INTO ssl_emp VALUES (2, 'Bob', 1, 100000)");
        $this->pdo->exec("INSERT INTO ssl_emp VALUES (3, 'Charlie', 2, 90000)");
        $this->pdo->exec("INSERT INTO ssl_emp VALUES (4, 'Diana', 2, 95000)");
    }
    /**
     * Scalar subquery in SELECT list.
     */
    public function testScalarSubqueryInSelectList(): void
    {
        $stmt = $this->pdo->query(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM ssl_emp e WHERE e.dept_id = d.id) AS emp_count
             FROM ssl_dept d
             ORDER BY d.name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $eng = array_filter($rows, fn($r) => $r['name'] === 'Engineering');
        $eng = reset($eng);
        $this->assertEquals(2, (int) $eng['emp_count']);

        $hr = array_filter($rows, fn($r) => $r['name'] === 'HR');
        $hr = reset($hr);
        $this->assertEquals(0, (int) $hr['emp_count']);
    }

    /**
     * Scalar subquery with aggregate (AVG) in SELECT list.
     */
    public function testScalarSubqueryWithAvg(): void
    {
        $stmt = $this->pdo->query(
            "SELECT d.name,
                    (SELECT AVG(salary) FROM ssl_emp WHERE dept_id = d.id) AS avg_salary
             FROM ssl_dept d
             WHERE d.id IN (1, 2)
             ORDER BY d.name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $eng = array_filter($rows, fn($r) => $r['name'] === 'Engineering');
        $eng = reset($eng);
        $this->assertEquals(110000, (float) $eng['avg_salary']);
    }

    /**
     * EXISTS subquery in WHERE.
     */
    public function testExistsSubqueryInWhere(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name FROM ssl_dept d
             WHERE EXISTS (SELECT 1 FROM ssl_emp WHERE dept_id = d.id)
             ORDER BY name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertCount(2, $rows);
        $this->assertContains('Engineering', $rows);
        $this->assertContains('Marketing', $rows);
        $this->assertNotContains('HR', $rows);
    }

    /**
     * NOT EXISTS subquery in WHERE.
     */
    public function testNotExistsSubqueryInWhere(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name FROM ssl_dept d
             WHERE NOT EXISTS (SELECT 1 FROM ssl_emp WHERE dept_id = d.id)"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertCount(1, $rows);
        $this->assertSame('HR', $rows[0]);
    }

    /**
     * Subquery in CASE WHEN expression.
     */
    public function testSubqueryInCaseWhen(): void
    {
        $stmt = $this->pdo->query(
            "SELECT d.name,
                    CASE WHEN (SELECT COUNT(*) FROM ssl_emp WHERE dept_id = d.id) > 0
                         THEN 'has employees'
                         ELSE 'empty'
                    END AS status
             FROM ssl_dept d
             ORDER BY d.name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $hr = array_filter($rows, fn($r) => $r['name'] === 'HR');
        $hr = reset($hr);
        $this->assertSame('empty', $hr['status']);

        $eng = array_filter($rows, fn($r) => $r['name'] === 'Engineering');
        $eng = reset($eng);
        $this->assertSame('has employees', $eng['status']);
    }

    /**
     * Subquery reflects shadow mutations.
     */
    public function testSubqueryReflectsMutations(): void
    {
        // Add employee to HR
        $this->pdo->exec("INSERT INTO ssl_emp VALUES (5, 'Eve', 3, 80000)");

        $stmt = $this->pdo->query(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM ssl_emp WHERE dept_id = d.id) AS cnt
             FROM ssl_dept d WHERE d.id = 3"
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(1, (int) $row['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ssl_emp');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
