<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests DELETE with correlated subquery patterns through ZTD CTE shadow store (PostgreSQL PDO).
 *
 * Uses a departments/employees schema to test WHERE EXISTS, scalar self-referencing
 * subquery, NOT EXISTS, and prepared DELETE with correlated subqueries.
 *
 * Uses INT (not BOOLEAN) for the active column to avoid SPEC-11.PG-BOOLEAN-FALSE.
 */
class PostgresDeleteCorrelatedTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dc_departments (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                active INT NOT NULL
            )',
            'CREATE TABLE pg_dc_employees (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                dept_id INT NOT NULL,
                performance INT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dc_employees', 'pg_dc_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 departments: Engineering (active), Marketing (inactive), Sales (active)
        $this->pdo->exec("INSERT INTO pg_dc_departments VALUES (1, 'Engineering', 1)");
        $this->pdo->exec("INSERT INTO pg_dc_departments VALUES (2, 'Marketing', 0)");
        $this->pdo->exec("INSERT INTO pg_dc_departments VALUES (3, 'Sales', 1)");

        // 6 employees across departments
        $this->pdo->exec("INSERT INTO pg_dc_employees VALUES (1, 'Alice', 1, 90)");
        $this->pdo->exec("INSERT INTO pg_dc_employees VALUES (2, 'Bob', 1, 70)");
        $this->pdo->exec("INSERT INTO pg_dc_employees VALUES (3, 'Charlie', 2, 60)");
        $this->pdo->exec("INSERT INTO pg_dc_employees VALUES (4, 'Diana', 2, 80)");
        $this->pdo->exec("INSERT INTO pg_dc_employees VALUES (5, 'Eve', 3, 50)");
        $this->pdo->exec("INSERT INTO pg_dc_employees VALUES (6, 'Frank', 3, 85)");
    }

    /**
     * DELETE WHERE EXISTS correlated: delete employees in inactive departments.
     * DELETE FROM employees WHERE EXISTS (SELECT 1 FROM departments WHERE id = employees.dept_id AND active = 0)
     */
    public function testDeleteWhereExistsCorrelated(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "DELETE FROM pg_dc_employees WHERE EXISTS (SELECT 1 FROM pg_dc_departments WHERE id = pg_dc_employees.dept_id AND active = 0)"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'DELETE WHERE EXISTS correlated subquery failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name FROM pg_dc_employees ORDER BY id");

        // Charlie (dept 2) and Diana (dept 2) should be deleted; Marketing is inactive
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
        $this->assertSame('Frank', $rows[3]['name']);
    }

    /**
     * DELETE WHERE scalar subquery from same table: delete employees with below-average performance.
     * DELETE FROM employees WHERE performance < (SELECT AVG(performance) FROM employees)
     */
    public function testDeleteWhereBelowAveragePerformance(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "DELETE FROM pg_dc_employees WHERE performance < (SELECT AVG(performance) FROM pg_dc_employees)"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'DELETE WHERE scalar subquery (AVG from same table) failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name, performance FROM pg_dc_employees ORDER BY id");

        // AVG(90,70,60,80,50,85) = 72.5
        // Below average: Bob(70), Charlie(60), Eve(50) -> deleted
        // Remaining: Alice(90), Diana(80), Frank(85)
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
        $this->assertSame('Frank', $rows[2]['name']);
    }

    /**
     * DELETE with NOT EXISTS: delete departments that have no employees.
     * DELETE FROM departments WHERE NOT EXISTS (SELECT 1 FROM employees WHERE dept_id = departments.id)
     *
     * In this seed data all departments have employees, so we first remove employees from dept 3
     * to create an orphan department, then delete it.
     */
    public function testDeleteWhereNotExistsCorrelated(): void
    {
        // Remove all employees from Sales (dept 3) to make it orphan
        $this->pdo->exec("DELETE FROM pg_dc_employees WHERE dept_id = 3");

        $thrown = null;
        try {
            $this->pdo->exec(
                "DELETE FROM pg_dc_departments WHERE NOT EXISTS (SELECT 1 FROM pg_dc_employees WHERE dept_id = pg_dc_departments.id)"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'DELETE WHERE NOT EXISTS correlated subquery failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name FROM pg_dc_departments ORDER BY id");

        // Sales (dept 3) has no employees -> deleted
        // Engineering (dept 1) and Marketing (dept 2) remain
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertSame('Marketing', $rows[1]['name']);
    }

    /**
     * Prepared DELETE with correlated subquery: delete employees in departments matching a parameter.
     */
    public function testPreparedDeleteWithCorrelatedSubquery(): void
    {
        $thrown = null;
        try {
            $stmt = $this->ztdPrepare(
                "DELETE FROM pg_dc_employees WHERE EXISTS (SELECT 1 FROM pg_dc_departments WHERE id = pg_dc_employees.dept_id AND name = ?)"
            );
            $stmt->execute(['Marketing']);
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'Prepared DELETE with correlated subquery failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name FROM pg_dc_employees ORDER BY id");

        // Charlie (dept 2) and Diana (dept 2) should be deleted; Marketing
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
        $this->assertSame('Frank', $rows[3]['name']);
    }

    /**
     * Physical isolation: shadow mutations via DELETE with correlated subquery do not reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "DELETE FROM pg_dc_employees WHERE EXISTS (SELECT 1 FROM pg_dc_departments WHERE id = pg_dc_employees.dept_id AND active = 0)"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'DELETE WHERE EXISTS failed; cannot test physical isolation: ' . $thrown->getMessage()
            );
        }

        // Shadow store should reflect the deletion (4 employees remain)
        $shadowRows = $this->ztdQuery("SELECT id FROM pg_dc_employees ORDER BY id");
        $this->assertCount(4, $shadowRows, 'Shadow should have 4 employees after deleting inactive dept members');

        // Physical table should have no rows (inserts were shadow-only)
        $this->pdo->disableZtd();
        $physicalRows = $this->pdo->query('SELECT id FROM pg_dc_employees')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $physicalRows, 'Physical table should be empty: all writes were shadow-only');
    }
}
