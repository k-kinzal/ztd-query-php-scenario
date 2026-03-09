<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests an employee scheduling workflow through ZTD shadow store (MySQLi).
 * Covers date range overlap detection with prepared statements, shift swapping,
 * department coverage aggregation, and physical isolation.
 * @spec SPEC-10.2.76
 */
class EmployeeSchedulingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_es_employees (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                department VARCHAR(50)
            )',
            'CREATE TABLE mi_es_shifts (
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
        return ['mi_es_shifts', 'mi_es_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 employees across 2 departments
        $this->mysqli->query("INSERT INTO mi_es_employees VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_es_employees VALUES (2, 'Bob', 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_es_employees VALUES (3, 'Charlie', 'Support')");
        $this->mysqli->query("INSERT INTO mi_es_employees VALUES (4, 'Diana', 'Support')");

        // 5 shifts
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (1, 1, '2026-03-09', '09:00', '17:00', 'confirmed')");
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (2, 2, '2026-03-09', '13:00', '21:00', 'confirmed')");
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (3, 3, '2026-03-09', '09:00', '17:00', 'confirmed')");
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (4, 1, '2026-03-10', '09:00', '17:00', 'confirmed')");
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (5, 4, '2026-03-10', '13:00', '21:00', 'confirmed')");
    }

    /**
     * List employee shifts within a date range using a prepared JOIN query.
     */
    public function testListEmployeeShifts(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name, s.shift_date, s.start_time, s.end_time
             FROM mi_es_shifts s
             JOIN mi_es_employees e ON e.id = s.employee_id
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
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (6, 2, '2026-03-10', '09:00', '17:00', 'confirmed')");

        $rows = $this->ztdQuery(
            "SELECT e.name, s.shift_date, s.start_time
             FROM mi_es_shifts s
             JOIN mi_es_employees e ON e.id = s.employee_id
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
        // Alice has 09:00-17:00 on 2026-03-09; check for overlap with 12:00-20:00
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.id, s.start_time, s.end_time
             FROM mi_es_shifts s
             WHERE s.employee_id = ? AND s.shift_date = ?
               AND s.start_time < ? AND s.end_time > ?",
            [1, '2026-03-09', '20:00', '12:00']
        );

        // Alice's 09:00-17:00 overlaps with 12:00-20:00 (start_time < 20:00 AND end_time > 12:00)
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);

        // Check no conflict with 18:00-22:00
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.id
             FROM mi_es_shifts s
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
        // Swap shift 1 (Alice) and shift 3 (Charlie) on 2026-03-09
        $this->mysqli->query("UPDATE mi_es_shifts SET employee_id = 3 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_es_shifts SET employee_id = 1 WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT s.id, e.name
             FROM mi_es_shifts s
             JOIN mi_es_employees e ON e.id = s.employee_id
             WHERE s.id IN (1, 3)
             ORDER BY s.id"
        );

        $this->assertCount(2, $rows);
        // Shift 1 now belongs to Charlie
        $this->assertSame('Charlie', $rows[0]['name']);
        // Shift 3 now belongs to Alice
        $this->assertSame('Alice', $rows[1]['name']);
    }

    /**
     * Count employees per shift grouped by department using HAVING.
     */
    public function testDepartmentCoverage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.department, s.shift_date, COUNT(DISTINCT e.id) AS staff_count
             FROM mi_es_shifts s
             JOIN mi_es_employees e ON e.id = s.employee_id
             WHERE s.status = 'confirmed'
             GROUP BY e.department, s.shift_date
             HAVING COUNT(DISTINCT e.id) >= 1
             ORDER BY s.shift_date, e.department"
        );

        // 2026-03-09: Engineering(2), Support(1); 2026-03-10: Engineering(1), Support(1)
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
        $countBefore = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_es_shifts WHERE shift_date = '2026-03-09'");
        $this->assertEquals(3, (int) $countBefore[0]['cnt']);

        // Cancel shift 2 (Bob, 2026-03-09)
        $this->mysqli->query("DELETE FROM mi_es_shifts WHERE id = 2");

        // Reassign Diana to cover
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (6, 4, '2026-03-09', '13:00', '21:00', 'confirmed')");

        $countAfter = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_es_shifts WHERE shift_date = '2026-03-09'");
        $this->assertEquals(3, (int) $countAfter[0]['cnt']);

        // Verify Diana is now on that shift
        $rows = $this->ztdQuery(
            "SELECT e.name FROM mi_es_shifts s
             JOIN mi_es_employees e ON e.id = s.employee_id
             WHERE s.id = 6"
        );
        $this->assertSame('Diana', $rows[0]['name']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_es_shifts VALUES (6, 2, '2026-03-11', '09:00', '17:00', 'confirmed')");
        $this->mysqli->query("UPDATE mi_es_employees SET department = 'QA' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_es_shifts");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT department FROM mi_es_employees WHERE id = 1");
        $this->assertSame('QA', $rows[0]['department']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_es_shifts');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
