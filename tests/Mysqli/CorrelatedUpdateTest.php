<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests correlated subqueries in SELECT, UPDATE, and DELETE through ZTD shadow store (MySQLi).
 * Covers correlated scalar subqueries, EXISTS/NOT EXISTS, IN subqueries, chained
 * correlated updates, and physical isolation.
 * @spec SPEC-10.2.88
 */
class CorrelatedUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cu_departments (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                budget DECIMAL(12,2),
                avg_salary DECIMAL(10,2)
            )',
            'CREATE TABLE mi_cu_employees (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                department_id INT,
                salary DECIMAL(10,2),
                bonus DECIMAL(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cu_employees', 'mi_cu_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 departments
        $this->mysqli->query("INSERT INTO mi_cu_departments VALUES (1, 'Engineering', 100000.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_cu_departments VALUES (2, 'Marketing', 60000.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_cu_departments VALUES (3, 'Research', 80000.00, 0.00)");

        // 6 employees across departments
        $this->mysqli->query("INSERT INTO mi_cu_employees VALUES (1, 'Alice', 1, 90000.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_cu_employees VALUES (2, 'Bob', 1, 85000.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_cu_employees VALUES (3, 'Charlie', 2, 55000.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_cu_employees VALUES (4, 'Diana', 2, 60000.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_cu_employees VALUES (5, 'Eve', 3, 75000.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_cu_employees VALUES (6, 'Frank', 3, 70000.00, 0.00)");
    }

    /**
     * Correlated subquery in SELECT list: look up department name for each employee.
     */
    public function testCorrelatedSubqueryInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    (SELECT d.name FROM mi_cu_departments d WHERE d.id = e.department_id) AS dept_name
             FROM mi_cu_employees e
             ORDER BY e.id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Engineering', $rows[1]['dept_name']);
        $this->assertSame('Marketing', $rows[2]['dept_name']);
        $this->assertSame('Marketing', $rows[3]['dept_name']);
        $this->assertSame('Research', $rows[4]['dept_name']);
        $this->assertSame('Research', $rows[5]['dept_name']);
    }

    /**
     * UPDATE with scalar correlated subquery: set avg_salary from employees table.
     */
    public function testUpdateWithScalarSubquery(): void
    {
        $this->mysqli->query(
            "UPDATE mi_cu_departments SET avg_salary = (SELECT AVG(salary) FROM mi_cu_employees WHERE department_id = mi_cu_departments.id)"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name, avg_salary FROM mi_cu_departments ORDER BY id"
        );

        $this->assertCount(3, $rows);
        // Engineering: (90000 + 85000) / 2 = 87500
        $this->assertEquals(87500.00, (float) $rows[0]['avg_salary']);
        // Marketing: (55000 + 60000) / 2 = 57500
        $this->assertEquals(57500.00, (float) $rows[1]['avg_salary']);
        // Research: (75000 + 70000) / 2 = 72500
        $this->assertEquals(72500.00, (float) $rows[2]['avg_salary']);
    }

    /**
     * DELETE with NOT EXISTS correlated subquery: remove departments with no employees.
     * First remove all employees from one department, then delete empty departments.
     */
    public function testDeleteWithExistsSubquery(): void
    {
        // Remove all Research employees
        $this->mysqli->query("DELETE FROM mi_cu_employees WHERE department_id = 3");

        // Delete departments that have no employees
        $this->mysqli->query(
            "DELETE FROM mi_cu_departments WHERE NOT EXISTS (SELECT 1 FROM mi_cu_employees WHERE department_id = mi_cu_departments.id)"
        );

        $rows = $this->ztdQuery("SELECT id, name FROM mi_cu_departments ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertSame('Marketing', $rows[1]['name']);
    }

    /**
     * UPDATE with WHERE IN subquery: set bonus for employees in high-budget departments.
     */
    public function testUpdateWhereInSubquery(): void
    {
        // Budget > 70000 matches Engineering (100000) and Research (80000), not Marketing (60000)
        $this->mysqli->query(
            "UPDATE mi_cu_employees SET bonus = salary * 0.1 WHERE department_id IN (SELECT id FROM mi_cu_departments WHERE budget > 70000)"
        );

        $rows = $this->ztdQuery("SELECT id, name, bonus FROM mi_cu_employees ORDER BY id");

        // Engineering employees get bonus
        $this->assertEquals(9000.00, (float) $rows[0]['bonus']);  // Alice: 90000 * 0.1
        $this->assertEquals(8500.00, (float) $rows[1]['bonus']);  // Bob: 85000 * 0.1
        // Marketing employees: no bonus
        $this->assertEquals(0.00, (float) $rows[2]['bonus']);     // Charlie
        $this->assertEquals(0.00, (float) $rows[3]['bonus']);     // Diana
        // Research employees get bonus
        $this->assertEquals(7500.00, (float) $rows[4]['bonus']);  // Eve: 75000 * 0.1
        $this->assertEquals(7000.00, (float) $rows[5]['bonus']);  // Frank: 70000 * 0.1
    }

    /**
     * Two sequential correlated updates: first set bonuses, then update department avg_salary
     * to include bonuses.
     */
    public function testChainedCorrelatedUpdates(): void
    {
        // Step 1: give all employees a 10% bonus
        $this->mysqli->query("UPDATE mi_cu_employees SET bonus = salary * 0.1");

        // Step 2: update avg_salary to average of (salary + bonus) per department
        $this->mysqli->query(
            "UPDATE mi_cu_departments SET avg_salary = (SELECT AVG(salary + bonus) FROM mi_cu_employees WHERE department_id = mi_cu_departments.id)"
        );

        $rows = $this->ztdQuery("SELECT id, name, avg_salary FROM mi_cu_departments ORDER BY id");

        // Engineering: AVG((90000+9000) + (85000+8500)) / 2 = AVG(99000, 93500) = 96250
        $this->assertEquals(96250.00, (float) $rows[0]['avg_salary']);
        // Marketing: AVG((55000+5500) + (60000+6000)) / 2 = AVG(60500, 66000) = 63250
        $this->assertEquals(63250.00, (float) $rows[1]['avg_salary']);
        // Research: AVG((75000+7500) + (70000+7000)) / 2 = AVG(82500, 77000) = 79750
        $this->assertEquals(79750.00, (float) $rows[2]['avg_salary']);
    }

    /**
     * Correlated COUNT subquery in SELECT list: count employees per department.
     */
    public function testCorrelatedCountInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM mi_cu_employees WHERE department_id = d.id) AS emp_count
             FROM mi_cu_departments d
             ORDER BY d.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['emp_count']);
        $this->assertSame('Marketing', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['emp_count']);
        $this->assertSame('Research', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['emp_count']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_cu_employees SET bonus = salary * 0.1");
        $this->mysqli->query(
            "UPDATE mi_cu_departments SET avg_salary = (SELECT AVG(salary) FROM mi_cu_employees WHERE department_id = mi_cu_departments.id)"
        );

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT bonus FROM mi_cu_employees WHERE id = 1");
        $this->assertEquals(9000.00, (float) $rows[0]['bonus']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT bonus FROM mi_cu_employees WHERE id = 1');
        $this->assertEquals(0.00, (float) $result->fetch_assoc()['bonus']);
    }
}
