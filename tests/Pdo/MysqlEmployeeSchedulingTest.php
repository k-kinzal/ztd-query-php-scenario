<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests an employee scheduling workflow through ZTD shadow store (MySQL PDO).
 * Covers date range overlap detection with prepared statements, shift swapping,
 * department coverage aggregation, and physical isolation.
 * @spec SPEC-10.2.76
 */
class MysqlEmployeeSchedulingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_es_employees (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                department VARCHAR(50)
            )',
            'CREATE TABLE mp_es_shifts (
                id INT PRIMARY KEY,
                employee_id INT,
                shift_date DATE,
                start_time VARCHAR(5),
                end_time VARCHAR(5),
                status VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_es_shifts', 'mp_es_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_es_employees VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_es_employees VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_es_employees VALUES (3, 'Charlie', 'Support')");
        $this->pdo->exec("INSERT INTO mp_es_employees VALUES (4, 'Diana', 'Support')");

        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (1, 1, '2026-03-09', '09:00', '17:00', 'confirmed')");
        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (2, 2, '2026-03-09', '13:00', '21:00', 'confirmed')");
        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (3, 3, '2026-03-09', '09:00', '17:00', 'confirmed')");
        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (4, 1, '2026-03-10', '09:00', '17:00', 'confirmed')");
        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (5, 4, '2026-03-10', '13:00', '21:00', 'confirmed')");
    }

    /**
     * List employee shifts within a date range using a prepared JOIN query.
     */
    public function testListEmployeeShifts(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name, s.shift_date, s.start_time, s.end_time
             FROM mp_es_shifts s
             JOIN mp_es_employees e ON e.id = s.employee_id
             WHERE s.shift_date BETWEEN ? AND ?
             ORDER BY s.shift_date, s.start_time, e.name",
            ['2026-03-09', '2026-03-09']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('09:00', $rows[0]['start_time']);
        $this->assertSame('Charlie', $rows[1]['name']);
        $this->assertSame('09:00', $rows[1]['start_time']);
        $this->assertSame('Bob', $rows[2]['name']);
    }

    /**
     * Assign a new shift via INSERT and verify it appears in the JOIN query.
     */
    public function testAssignShift(): void
    {
        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (6, 2, '2026-03-10', '09:00', '17:00', 'confirmed')");

        $rows = $this->ztdQuery(
            "SELECT e.name, s.shift_date, s.start_time
             FROM mp_es_shifts s
             JOIN mp_es_employees e ON e.id = s.employee_id
             WHERE s.id = 6"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('2026-03-10', $rows[0]['shift_date']);
    }

    /**
     * Detect a schedule conflict by querying overlapping shifts for an employee on a given date.
     */
    public function testDetectScheduleConflict(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.id, s.start_time, s.end_time
             FROM mp_es_shifts s
             WHERE s.employee_id = ? AND s.shift_date = ?
               AND s.start_time < ? AND s.end_time > ?",
            [1, '2026-03-09', '20:00', '12:00']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.id
             FROM mp_es_shifts s
             WHERE s.employee_id = ? AND s.shift_date = ?
               AND s.start_time < ? AND s.end_time > ?",
            [1, '2026-03-09', '22:00', '18:00']
        );

        $this->assertCount(0, $rows);
    }

    /**
     * Swap shifts between two employees and verify both changed.
     */
    public function testSwapShifts(): void
    {
        $this->pdo->exec("UPDATE mp_es_shifts SET employee_id = 3 WHERE id = 1");
        $this->pdo->exec("UPDATE mp_es_shifts SET employee_id = 1 WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT s.id, e.name
             FROM mp_es_shifts s
             JOIN mp_es_employees e ON e.id = s.employee_id
             WHERE s.id IN (1, 3)
             ORDER BY s.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('Alice', $rows[1]['name']);
    }

    /**
     * Count employees per shift grouped by department using HAVING.
     */
    public function testDepartmentCoverage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.department, s.shift_date, COUNT(DISTINCT e.id) AS staff_count
             FROM mp_es_shifts s
             JOIN mp_es_employees e ON e.id = s.employee_id
             WHERE s.status = 'confirmed'
             GROUP BY e.department, s.shift_date
             HAVING COUNT(DISTINCT e.id) >= 1
             ORDER BY s.shift_date, e.department"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('2026-03-09', $rows[0]['shift_date']);
        $this->assertEquals(2, (int) $rows[0]['staff_count']);

        $this->assertSame('Support', $rows[1]['department']);
        $this->assertEquals(1, (int) $rows[1]['staff_count']);
    }

    /**
     * Cancel a shift (DELETE) and reassign a replacement (INSERT), verifying count unchanged.
     */
    public function testCancelAndReassign(): void
    {
        $countBefore = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_es_shifts WHERE shift_date = '2026-03-09'");
        $this->assertEquals(3, (int) $countBefore[0]['cnt']);

        $this->pdo->exec("DELETE FROM mp_es_shifts WHERE id = 2");
        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (6, 4, '2026-03-09', '13:00', '21:00', 'confirmed')");

        $countAfter = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_es_shifts WHERE shift_date = '2026-03-09'");
        $this->assertEquals(3, (int) $countAfter[0]['cnt']);

        $rows = $this->ztdQuery(
            "SELECT e.name FROM mp_es_shifts s
             JOIN mp_es_employees e ON e.id = s.employee_id
             WHERE s.id = 6"
        );
        $this->assertSame('Diana', $rows[0]['name']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_es_shifts VALUES (6, 2, '2026-03-11', '09:00', '17:00', 'confirmed')");
        $this->pdo->exec("UPDATE mp_es_employees SET department = 'QA' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_es_shifts");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT department FROM mp_es_employees WHERE id = 1");
        $this->assertSame('QA', $rows[0]['department']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_es_shifts")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
