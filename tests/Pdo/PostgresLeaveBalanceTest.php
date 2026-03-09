<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests an employee leave balance scenario through ZTD shadow store (PostgreSQL PDO).
 * Employees request leave of various types; balance calculation, overlap detection,
 * and department summaries exercise 3-table JOINs, LEFT JOIN with COALESCE,
 * self-join for overlap detection, SUM CASE for cross-tab aggregation,
 * UPDATE with verification, prepared statement for date range search,
 * and physical isolation check.
 * @spec SPEC-10.2.155
 */
class PostgresLeaveBalanceTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_lv_employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                department VARCHAR(50),
                hire_date TEXT
            )',
            'CREATE TABLE pg_lv_leave_types (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                annual_days INTEGER
            )',
            'CREATE TABLE pg_lv_leave_requests (
                id SERIAL PRIMARY KEY,
                employee_id INTEGER,
                leave_type_id INTEGER,
                start_date TEXT,
                end_date TEXT,
                days INTEGER,
                status VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_lv_leave_requests', 'pg_lv_employees', 'pg_lv_leave_types'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 employees
        $this->pdo->exec("INSERT INTO pg_lv_employees VALUES (1, 'Alice', 'Engineering', '2024-01-15')");
        $this->pdo->exec("INSERT INTO pg_lv_employees VALUES (2, 'Bob', 'Marketing', '2024-06-01')");
        $this->pdo->exec("INSERT INTO pg_lv_employees VALUES (3, 'Carol', 'Engineering', '2025-01-10')");
        $this->pdo->exec("INSERT INTO pg_lv_employees VALUES (4, 'Dave', 'Marketing', '2023-03-20')");

        // 3 leave types
        $this->pdo->exec("INSERT INTO pg_lv_leave_types VALUES (1, 'Annual Leave', 20)");
        $this->pdo->exec("INSERT INTO pg_lv_leave_types VALUES (2, 'Sick Leave', 10)");
        $this->pdo->exec("INSERT INTO pg_lv_leave_types VALUES (3, 'Personal Leave', 5)");

        // 10 leave requests
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (1, 1, 1, '2026-01-06', '2026-01-10', 5, 'approved')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (2, 1, 2, '2026-02-03', '2026-02-04', 2, 'approved')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (3, 2, 1, '2026-01-13', '2026-01-17', 5, 'approved')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (4, 2, 1, '2026-03-02', '2026-03-06', 5, 'pending')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (5, 3, 1, '2026-02-17', '2026-02-21', 5, 'approved')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (6, 3, 3, '2026-03-10', '2026-03-11', 2, 'pending')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (7, 4, 1, '2026-01-20', '2026-01-31', 10, 'approved')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (8, 4, 2, '2026-02-10', '2026-02-12', 3, 'approved')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (9, 1, 1, '2026-03-17', '2026-03-21', 5, 'pending')");
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (10, 4, 1, '2026-03-03', '2026-03-07', 5, 'rejected')");
    }

    /**
     * 3-table JOIN: employees + leave_requests + leave_types, WHERE status = 'approved',
     * GROUP BY employee name, SUM(days), ORDER BY total DESC.
     * Expected: Dave=13, Alice=7, Bob=5, Carol=5.
     */
    public function testApprovedLeaveByEmployee(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, SUM(lr.days) AS total_days
             FROM pg_lv_leave_requests lr
             JOIN pg_lv_employees e ON e.id = lr.employee_id
             JOIN pg_lv_leave_types lt ON lt.id = lr.leave_type_id
             WHERE lr.status = 'approved'
             GROUP BY e.name
             ORDER BY total_days DESC, e.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Dave', $rows[0]['name']);
        $this->assertEquals(13, (int) $rows[0]['total_days']);

        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertEquals(7, (int) $rows[1]['total_days']);

        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertEquals(5, (int) $rows[2]['total_days']);

        $this->assertSame('Carol', $rows[3]['name']);
        $this->assertEquals(5, (int) $rows[3]['total_days']);
    }

    /**
     * Remaining balance by type for employee_id=1 (Alice).
     * LEFT JOIN leave_types to leave_requests filtered to employee + approved,
     * COALESCE(SUM(days), 0) for used.
     * Expected: Annual Leave 20/5/15, Sick Leave 10/2/8, Personal Leave 5/0/5.
     */
    public function testRemainingBalanceByType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT lt.name, lt.annual_days,
                    COALESCE(SUM(lr.days), 0) AS used_days,
                    lt.annual_days - COALESCE(SUM(lr.days), 0) AS remaining
             FROM pg_lv_leave_types lt
             LEFT JOIN pg_lv_leave_requests lr
                 ON lr.leave_type_id = lt.id
                 AND lr.employee_id = 1
                 AND lr.status = 'approved'
             GROUP BY lt.id, lt.name, lt.annual_days
             ORDER BY lt.id"
        );

        $this->assertCount(3, $rows);

        // Annual Leave: 20 annual, 5 used, 15 remaining
        $this->assertSame('Annual Leave', $rows[0]['name']);
        $this->assertEquals(20, (int) $rows[0]['annual_days']);
        $this->assertEquals(5, (int) $rows[0]['used_days']);
        $this->assertEquals(15, (int) $rows[0]['remaining']);

        // Sick Leave: 10 annual, 2 used, 8 remaining
        $this->assertSame('Sick Leave', $rows[1]['name']);
        $this->assertEquals(10, (int) $rows[1]['annual_days']);
        $this->assertEquals(2, (int) $rows[1]['used_days']);
        $this->assertEquals(8, (int) $rows[1]['remaining']);

        // Personal Leave: 5 annual, 0 used, 5 remaining
        $this->assertSame('Personal Leave', $rows[2]['name']);
        $this->assertEquals(5, (int) $rows[2]['annual_days']);
        $this->assertEquals(0, (int) $rows[2]['used_days']);
        $this->assertEquals(5, (int) $rows[2]['remaining']);
    }

    /**
     * Overlap detection: self-join on leave_requests for same employee,
     * both approved, overlapping date ranges (start_date <= other.end_date
     * AND end_date >= other.start_date AND id < other.id).
     * No overlapping approved requests exist in test data, so expect 0 rows.
     */
    public function testOverlappingRequestDetection(): void
    {
        $rows = $this->ztdQuery(
            "SELECT lr1.id AS request1_id, lr2.id AS request2_id,
                    lr1.employee_id
             FROM pg_lv_leave_requests lr1
             JOIN pg_lv_leave_requests lr2
                 ON lr1.employee_id = lr2.employee_id
                 AND lr1.id < lr2.id
                 AND lr1.start_date <= lr2.end_date
                 AND lr1.end_date >= lr2.start_date
             WHERE lr1.status = 'approved'
               AND lr2.status = 'approved'"
        );

        $this->assertCount(0, $rows);
    }

    /**
     * Department leave overview: GROUP BY department, SUM CASE for approved/pending/rejected days.
     * Expected:
     * - Engineering: approved=12, pending=7, rejected=0
     * - Marketing: approved=18, pending=5, rejected=5
     * ORDER BY department.
     */
    public function testDepartmentLeaveOverview(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.department,
                    SUM(CASE WHEN lr.status = 'approved' THEN lr.days ELSE 0 END) AS approved_days,
                    SUM(CASE WHEN lr.status = 'pending' THEN lr.days ELSE 0 END) AS pending_days,
                    SUM(CASE WHEN lr.status = 'rejected' THEN lr.days ELSE 0 END) AS rejected_days
             FROM pg_lv_leave_requests lr
             JOIN pg_lv_employees e ON e.id = lr.employee_id
             GROUP BY e.department
             ORDER BY e.department"
        );

        $this->assertCount(2, $rows);

        // Engineering: approved=12, pending=7, rejected=0
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertEquals(12, (int) $rows[0]['approved_days']);
        $this->assertEquals(7, (int) $rows[0]['pending_days']);
        $this->assertEquals(0, (int) $rows[0]['rejected_days']);

        // Marketing: approved=18, pending=5, rejected=5
        $this->assertSame('Marketing', $rows[1]['department']);
        $this->assertEquals(18, (int) $rows[1]['approved_days']);
        $this->assertEquals(5, (int) $rows[1]['pending_days']);
        $this->assertEquals(5, (int) $rows[1]['rejected_days']);
    }

    /**
     * Approve a pending request and verify the updated balance.
     * UPDATE leave_requests SET status = 'approved' WHERE id = 9 (Alice's pending annual leave).
     * Then verify Alice's remaining Annual Leave is now 10 (20 - 5 - 5).
     */
    public function testApproveAndVerifyBalance(): void
    {
        // Approve Alice's pending annual leave request (id=9)
        $this->pdo->exec("UPDATE pg_lv_leave_requests SET status = 'approved' WHERE id = 9");

        // Verify Alice's remaining Annual Leave balance
        $rows = $this->ztdQuery(
            "SELECT lt.annual_days,
                    SUM(lr.days) AS used_days,
                    lt.annual_days - SUM(lr.days) AS remaining
             FROM pg_lv_leave_requests lr
             JOIN pg_lv_leave_types lt ON lt.id = lr.leave_type_id
             WHERE lr.employee_id = 1
               AND lr.leave_type_id = 1
               AND lr.status = 'approved'
             GROUP BY lt.annual_days"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(20, (int) $rows[0]['annual_days']);
        $this->assertEquals(10, (int) $rows[0]['used_days']);
        $this->assertEquals(10, (int) $rows[0]['remaining']);
    }

    /**
     * Prepared statement: find approved leave in a date range.
     * Params: start_date >= '2026-01-01' AND end_date <= '2026-01-31'.
     * Expected 3 rows: Alice Jan 6-10, Bob Jan 13-17, Dave Jan 20-31.
     * ORDER BY start_date.
     */
    public function testPreparedDateRangeSearch(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name, lr.start_date, lr.end_date, lr.days
             FROM pg_lv_leave_requests lr
             JOIN pg_lv_employees e ON e.id = lr.employee_id
             WHERE lr.status = 'approved'
               AND lr.start_date >= ?
               AND lr.end_date <= ?
             ORDER BY lr.start_date",
            ['2026-01-01', '2026-01-31']
        );

        $this->assertCount(3, $rows);

        // Alice Jan 6-10
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('2026-01-06', $rows[0]['start_date']);
        $this->assertSame('2026-01-10', $rows[0]['end_date']);
        $this->assertEquals(5, (int) $rows[0]['days']);

        // Bob Jan 13-17
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('2026-01-13', $rows[1]['start_date']);
        $this->assertSame('2026-01-17', $rows[1]['end_date']);
        $this->assertEquals(5, (int) $rows[1]['days']);

        // Dave Jan 20-31
        $this->assertSame('Dave', $rows[2]['name']);
        $this->assertSame('2026-01-20', $rows[2]['start_date']);
        $this->assertSame('2026-01-31', $rows[2]['end_date']);
        $this->assertEquals(10, (int) $rows[2]['days']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new leave request via shadow
        $this->pdo->exec("INSERT INTO pg_lv_leave_requests VALUES (11, 2, 2, '2026-04-01', '2026-04-03', 3, 'pending')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_lv_leave_requests");
        $this->assertEquals(11, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_lv_leave_requests")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
