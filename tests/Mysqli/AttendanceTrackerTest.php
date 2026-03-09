<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests an employee attendance tracking scenario: status categorization,
 * monthly stats, absence detection, and date-range filtering.
 * SQL patterns exercised: SUM CASE for status categorization, GROUP BY for
 * department rates, LEFT JOIN anti-pattern for absence detection,
 * prepared BETWEEN for date ranges, HAVING for filtering aggregates (MySQLi).
 * @spec SPEC-10.2.142
 */
class AttendanceTrackerTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_at_employees (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                department VARCHAR(100)
            )',
            'CREATE TABLE mi_at_attendance (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_id INT,
                attend_date TEXT,
                status VARCHAR(20),
                check_in_time TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_at_attendance', 'mi_at_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Employees
        $this->mysqli->query("INSERT INTO mi_at_employees VALUES (1, 'Alice Chen', 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_at_employees VALUES (2, 'Bob Park', 'Marketing')");
        $this->mysqli->query("INSERT INTO mi_at_employees VALUES (3, 'Carol Kim', 'Engineering')");

        // Attendance — August 2025
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (1, 1, '2025-08-01', 'present', '08:55')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (2, 1, '2025-08-02', 'present', '08:50')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (3, 1, '2025-08-03', 'late', '09:15')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (4, 1, '2025-08-04', 'present', '08:45')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (5, 1, '2025-08-05', 'absent', NULL)");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (6, 2, '2025-08-01', 'present', '08:58')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (7, 2, '2025-08-02', 'late', '09:20')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (8, 2, '2025-08-03', 'late', '09:10')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (9, 2, '2025-08-04', 'absent', NULL)");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (10, 2, '2025-08-05', 'present', '08:40')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (11, 3, '2025-08-01', 'present', '08:30')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (12, 3, '2025-08-02', 'present', '08:45')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (13, 3, '2025-08-03', 'present', '08:50')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (14, 3, '2025-08-04', 'present', '08:35')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (15, 3, '2025-08-05', 'present', '08:40')");

        // Attendance — September 2025
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (16, 1, '2025-09-01', 'present', '08:50')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (17, 1, '2025-09-02', 'late', '09:05')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (18, 2, '2025-09-01', 'absent', NULL)");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (19, 2, '2025-09-02', 'present', '08:55')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (20, 3, '2025-09-01', 'present', '08:30')");
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (21, 3, '2025-09-02', 'present', '08:35')");
    }

    /**
     * SUM CASE for each status per employee: present, late, absent counts.
     * Alice: present=4, late=2, absent=1
     * Bob: present=3, late=2, absent=2
     * Carol: present=7, late=0, absent=0
     */
    public function testAttendanceSummaryPerEmployee(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    SUM(CASE WHEN a.status = 'present' THEN 1 ELSE 0 END) AS present_count,
                    SUM(CASE WHEN a.status = 'late' THEN 1 ELSE 0 END) AS late_count,
                    SUM(CASE WHEN a.status = 'absent' THEN 1 ELSE 0 END) AS absent_count
             FROM mi_at_employees e
             JOIN mi_at_attendance a ON e.id = a.employee_id
             GROUP BY e.name
             ORDER BY e.name"
        );

        $this->assertCount(3, $rows);

        // Alice Chen: 4 present, 2 late, 1 absent
        $this->assertSame('Alice Chen', $rows[0]['name']);
        $this->assertEquals(4, (int) $rows[0]['present_count']);
        $this->assertEquals(2, (int) $rows[0]['late_count']);
        $this->assertEquals(1, (int) $rows[0]['absent_count']);

        // Bob Park: 3 present, 2 late, 2 absent
        $this->assertSame('Bob Park', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['present_count']);
        $this->assertEquals(2, (int) $rows[1]['late_count']);
        $this->assertEquals(2, (int) $rows[1]['absent_count']);

        // Carol Kim: 7 present, 0 late, 0 absent
        $this->assertSame('Carol Kim', $rows[2]['name']);
        $this->assertEquals(7, (int) $rows[2]['present_count']);
        $this->assertEquals(0, (int) $rows[2]['late_count']);
        $this->assertEquals(0, (int) $rows[2]['absent_count']);
    }

    /**
     * GROUP BY department, attendance rate = (present+late) / total * 100.
     * Engineering: 14 records, 13 present+late, 1 absent => 92.9%
     * Marketing: 7 records, 5 present+late, 2 absent => 71.4%
     */
    public function testDepartmentAttendanceRate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.department,
                    COUNT(*) AS total_records,
                    SUM(CASE WHEN a.status IN ('present', 'late') THEN 1 ELSE 0 END) AS attended,
                    ROUND(SUM(CASE WHEN a.status IN ('present', 'late') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS attendance_rate
             FROM mi_at_employees e
             JOIN mi_at_attendance a ON e.id = a.employee_id
             GROUP BY e.department
             ORDER BY e.department"
        );

        $this->assertCount(2, $rows);

        // Engineering: 14 total, 13 attended, 92.9%
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertEquals(14, (int) $rows[0]['total_records']);
        $this->assertEquals(13, (int) $rows[0]['attended']);
        $this->assertEqualsWithDelta(92.9, (float) $rows[0]['attendance_rate'], 0.1);

        // Marketing: 7 total, 5 attended, 71.4%
        $this->assertSame('Marketing', $rows[1]['department']);
        $this->assertEquals(7, (int) $rows[1]['total_records']);
        $this->assertEquals(5, (int) $rows[1]['attended']);
        $this->assertEqualsWithDelta(71.4, (float) $rows[1]['attendance_rate'], 0.1);
    }

    /**
     * Prepared BETWEEN: select employee name and status for a date range.
     * '2025-08-03' to '2025-08-05' => 9 records (3 employees x 3 days).
     */
    public function testDateRangeQuery(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name, a.attend_date, a.status
             FROM mi_at_employees e
             JOIN mi_at_attendance a ON e.id = a.employee_id
             WHERE a.attend_date BETWEEN ? AND ?
             ORDER BY a.attend_date, e.name",
            ['2025-08-03', '2025-08-05']
        );

        $this->assertCount(9, $rows);

        // Aug 3: Alice (late), Bob (late), Carol (present)
        $this->assertSame('Alice Chen', $rows[0]['name']);
        $this->assertSame('2025-08-03', $rows[0]['attend_date']);
        $this->assertSame('late', $rows[0]['status']);

        $this->assertSame('Bob Park', $rows[1]['name']);
        $this->assertSame('2025-08-03', $rows[1]['attend_date']);
        $this->assertSame('late', $rows[1]['status']);

        $this->assertSame('Carol Kim', $rows[2]['name']);
        $this->assertSame('2025-08-03', $rows[2]['attend_date']);
        $this->assertSame('present', $rows[2]['status']);

        // Aug 5: Alice (absent), Bob (present), Carol (present)
        $this->assertSame('Alice Chen', $rows[6]['name']);
        $this->assertSame('2025-08-05', $rows[6]['attend_date']);
        $this->assertSame('absent', $rows[6]['status']);

        $this->assertSame('Bob Park', $rows[7]['name']);
        $this->assertSame('2025-08-05', $rows[7]['attend_date']);
        $this->assertSame('present', $rows[7]['status']);

        $this->assertSame('Carol Kim', $rows[8]['name']);
        $this->assertSame('2025-08-05', $rows[8]['attend_date']);
        $this->assertSame('present', $rows[8]['status']);
    }

    /**
     * LEFT JOIN anti-pattern: find employees with NO attendance record on a date.
     * '2025-08-06' has no records for anyone => all 3 employees returned.
     */
    public function testAbsentEmployeesOnDate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name
             FROM mi_at_employees e
             LEFT JOIN mi_at_attendance a ON e.id = a.employee_id AND a.attend_date = '2025-08-06'
             WHERE a.id IS NULL
             ORDER BY e.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice Chen', $rows[0]['name']);
        $this->assertSame('Bob Park', $rows[1]['name']);
        $this->assertSame('Carol Kim', $rows[2]['name']);
    }

    /**
     * HAVING with SUM CASE: find employees with zero absences.
     * Only Carol Kim has 0 absences.
     */
    public function testPerfectAttendanceEmployees(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name
             FROM mi_at_employees e
             JOIN mi_at_attendance a ON e.id = a.employee_id
             GROUP BY e.name
             HAVING SUM(CASE WHEN a.status = 'absent' THEN 1 ELSE 0 END) = 0
             ORDER BY e.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Carol Kim', $rows[0]['name']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_at_attendance VALUES (22, 1, '2025-09-03', 'present', '08:50')");

        // ZTD sees the new record
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_at_attendance");
        $this->assertEquals(22, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_at_attendance');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
