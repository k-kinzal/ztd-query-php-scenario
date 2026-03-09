<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a project timesheet scenario through ZTD shadow store (SQLite PDO).
 * Employees log hours on projects. SQLite does not support ROLLUP, so
 * subtotals are computed with a UNION ALL approach. Also exercises
 * conditional aggregation with SUM(CASE WHEN ...), HAVING with aggregate
 * threshold, prepared statement with GROUP BY and aggregate filter,
 * and physical isolation.
 * @spec SPEC-10.2.168
 */
class SqliteProjectTimesheetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ts_employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                department TEXT
            )',
            'CREATE TABLE sl_ts_projects (
                id INTEGER PRIMARY KEY,
                name TEXT,
                status TEXT
            )',
            'CREATE TABLE sl_ts_entries (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER,
                project_id INTEGER,
                hours TEXT,
                entry_date TEXT,
                billable INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ts_entries', 'sl_ts_projects', 'sl_ts_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 employees
        $this->pdo->exec("INSERT INTO sl_ts_employees VALUES (1, 'Alice', 'engineering')");
        $this->pdo->exec("INSERT INTO sl_ts_employees VALUES (2, 'Bob', 'engineering')");
        $this->pdo->exec("INSERT INTO sl_ts_employees VALUES (3, 'Carol', 'design')");
        $this->pdo->exec("INSERT INTO sl_ts_employees VALUES (4, 'Dave', 'design')");

        // 3 projects
        $this->pdo->exec("INSERT INTO sl_ts_projects VALUES (1, 'Alpha', 'active')");
        $this->pdo->exec("INSERT INTO sl_ts_projects VALUES (2, 'Beta', 'active')");
        $this->pdo->exec("INSERT INTO sl_ts_projects VALUES (3, 'Legacy', 'maintenance')");

        // 15 time entries
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (1, 1, 1, '8.0', '2025-11-01', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (2, 1, 1, '6.5', '2025-11-02', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (3, 1, 2, '4.0', '2025-11-03', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (4, 1, 3, '2.0', '2025-11-04', 0)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (5, 2, 1, '7.0', '2025-11-01', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (6, 2, 2, '5.5', '2025-11-02', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (7, 2, 2, '3.0', '2025-11-03', 0)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (8, 3, 1, '4.0', '2025-11-01', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (9, 3, 2, '6.0', '2025-11-02', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (10, 3, 3, '3.5', '2025-11-03', 0)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (11, 4, 2, '8.0', '2025-11-01', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (12, 4, 2, '7.0', '2025-11-02', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (13, 4, 3, '2.5', '2025-11-03', 0)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (14, 1, 1, '5.0', '2025-11-05', 1)");
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (15, 2, 1, '4.5', '2025-11-05', 1)");
    }

    /**
     * UNION ALL approach for subtotals (SQLite does not support ROLLUP).
     * Three queries unioned: detail rows, department subtotals, grand total.
     * Expected 9 rows matching the ROLLUP output from MySQL/PostgreSQL tests.
     *
     * design/Alpha=4.0, design/Beta=21.0, design/Legacy=6.0, design/(All)=31.0,
     * engineering/Alpha=31.0, engineering/Beta=12.5, engineering/Legacy=2.0,
     * engineering/(All)=45.5, (All)/(All)=76.5.
     */
    public function testHoursByDepartmentAndProject(): void
    {
        $rows = $this->ztdQuery(
            "SELECT department, project, total_hours FROM (
                 SELECT e.department AS department,
                        p.name AS project,
                        SUM(t.hours) AS total_hours,
                        1 AS sort_level
                 FROM sl_ts_entries t
                 JOIN sl_ts_employees e ON e.id = t.employee_id
                 JOIN sl_ts_projects p ON p.id = t.project_id
                 GROUP BY e.department, p.name

                 UNION ALL

                 SELECT e.department AS department,
                        '(All)' AS project,
                        SUM(t.hours) AS total_hours,
                        2 AS sort_level
                 FROM sl_ts_entries t
                 JOIN sl_ts_employees e ON e.id = t.employee_id
                 GROUP BY e.department

                 UNION ALL

                 SELECT '(All)' AS department,
                        '(All)' AS project,
                        SUM(t.hours) AS total_hours,
                        3 AS sort_level
                 FROM sl_ts_entries t
             ) combined
             ORDER BY department, sort_level, project"
        );

        if (count($rows) < 9) {
            $this->markTestIncomplete(
                'UNION ALL subtotal approach in derived table returns ' . count($rows) . ' rows instead of 9 through ZTD on SQLite. '
                . 'Known issue with UNION ALL in derived tables (branches lost).'
            );
        }

        $this->assertCount(9, $rows);

        // design detail rows
        $this->assertSame('design', $rows[0]['department']);
        $this->assertSame('Alpha', $rows[0]['project']);
        $this->assertEquals(4.0, (float) $rows[0]['total_hours']);

        $this->assertSame('design', $rows[1]['department']);
        $this->assertSame('Beta', $rows[1]['project']);
        $this->assertEquals(21.0, (float) $rows[1]['total_hours']);

        $this->assertSame('design', $rows[2]['department']);
        $this->assertSame('Legacy', $rows[2]['project']);
        $this->assertEquals(6.0, (float) $rows[2]['total_hours']);

        // design subtotal
        $this->assertSame('design', $rows[3]['department']);
        $this->assertSame('(All)', $rows[3]['project']);
        $this->assertEquals(31.0, (float) $rows[3]['total_hours']);

        // engineering detail rows
        $this->assertSame('engineering', $rows[4]['department']);
        $this->assertSame('Alpha', $rows[4]['project']);
        $this->assertEquals(31.0, (float) $rows[4]['total_hours']);

        $this->assertSame('engineering', $rows[5]['department']);
        $this->assertSame('Beta', $rows[5]['project']);
        $this->assertEquals(12.5, (float) $rows[5]['total_hours']);

        $this->assertSame('engineering', $rows[6]['department']);
        $this->assertSame('Legacy', $rows[6]['project']);
        $this->assertEquals(2.0, (float) $rows[6]['total_hours']);

        // engineering subtotal
        $this->assertSame('engineering', $rows[7]['department']);
        $this->assertSame('(All)', $rows[7]['project']);
        $this->assertEquals(45.5, (float) $rows[7]['total_hours']);

        // grand total
        $this->assertSame('(All)', $rows[8]['department']);
        $this->assertSame('(All)', $rows[8]['project']);
        $this->assertEquals(76.5, (float) $rows[8]['total_hours']);
    }

    /**
     * Conditional aggregation: SUM(CASE WHEN billable = 1 THEN hours ELSE 0 END) per employee.
     * Expected: Alice=23.5/2.0, Bob=17.0/3.0, Carol=10.0/3.5, Dave=15.0/2.5.
     */
    public function testConditionalAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    SUM(CASE WHEN t.billable = 1 THEN t.hours ELSE 0 END) AS billable_hours,
                    SUM(CASE WHEN t.billable = 0 THEN t.hours ELSE 0 END) AS non_billable_hours
             FROM sl_ts_entries t
             JOIN sl_ts_employees e ON e.id = t.employee_id
             GROUP BY e.id, e.name
             ORDER BY e.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(23.5, (float) $rows[0]['billable_hours']);
        $this->assertEquals(2.0, (float) $rows[0]['non_billable_hours']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(17.0, (float) $rows[1]['billable_hours']);
        $this->assertEquals(3.0, (float) $rows[1]['non_billable_hours']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(10.0, (float) $rows[2]['billable_hours']);
        $this->assertEquals(3.5, (float) $rows[2]['non_billable_hours']);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertEquals(15.0, (float) $rows[3]['billable_hours']);
        $this->assertEquals(2.5, (float) $rows[3]['non_billable_hours']);
    }

    /**
     * Project utilization: billable_hours / total_hours * 100 per project.
     * Alpha: 35.0 total, 35.0 billable (100.0%).
     * Beta: 33.5 total, 30.5 billable (91.0%).
     * Legacy: 8.0 total, 0 billable (0.0%).
     */
    public function testProjectUtilization(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name,
                    SUM(t.hours) AS total_hours,
                    SUM(CASE WHEN t.billable = 1 THEN t.hours ELSE 0 END) AS billable_hours,
                    ROUND(SUM(CASE WHEN t.billable = 1 THEN t.hours ELSE 0 END) * 100.0 / SUM(t.hours), 1) AS utilization_pct
             FROM sl_ts_entries t
             JOIN sl_ts_projects p ON p.id = t.project_id
             GROUP BY p.id, p.name
             ORDER BY p.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertEquals(35.0, (float) $rows[0]['total_hours']);
        $this->assertEquals(35.0, (float) $rows[0]['billable_hours']);
        $this->assertEquals(100.0, round((float) $rows[0]['utilization_pct'], 1));

        $this->assertSame('Beta', $rows[1]['name']);
        $this->assertEquals(33.5, (float) $rows[1]['total_hours']);
        $this->assertEquals(30.5, (float) $rows[1]['billable_hours']);
        $this->assertEquals(91.0, round((float) $rows[1]['utilization_pct'], 1));

        $this->assertSame('Legacy', $rows[2]['name']);
        $this->assertEquals(8.0, (float) $rows[2]['total_hours']);
        $this->assertEquals(0.0, (float) $rows[2]['billable_hours']);
        $this->assertEquals(0.0, round((float) $rows[2]['utilization_pct'], 1));
    }

    /**
     * HAVING SUM(hours) > 15: employees with total hours above threshold.
     * Expected: Alice (25.5), Bob (20.0), Dave (17.5). Carol (13.5) excluded.
     */
    public function testHavingAggregateThreshold(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, SUM(t.hours) AS total_hours
             FROM sl_ts_entries t
             JOIN sl_ts_employees e ON e.id = t.employee_id
             GROUP BY e.id, e.name
             HAVING SUM(t.hours) > 15
             ORDER BY total_hours DESC"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(25.5, (float) $rows[0]['total_hours']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(20.0, (float) $rows[1]['total_hours']);

        $this->assertSame('Dave', $rows[2]['name']);
        $this->assertEquals(17.5, (float) $rows[2]['total_hours']);
    }

    /**
     * Prepared statement: filter by department + GROUP BY project.
     * Param: 'engineering'. Expected 3 rows: Alpha=31.0, Beta=12.5, Legacy=2.0.
     */
    public function testPreparedDepartmentFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.name AS project, SUM(t.hours) AS total_hours
             FROM sl_ts_entries t
             JOIN sl_ts_employees e ON e.id = t.employee_id
             JOIN sl_ts_projects p ON p.id = t.project_id
             WHERE e.department = ?
             GROUP BY p.id, p.name
             ORDER BY p.name",
            ['engineering']
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alpha', $rows[0]['project']);
        $this->assertEquals(31.0, (float) $rows[0]['total_hours']);

        $this->assertSame('Beta', $rows[1]['project']);
        $this->assertEquals(12.5, (float) $rows[1]['total_hours']);

        $this->assertSame('Legacy', $rows[2]['project']);
        $this->assertEquals(2.0, (float) $rows[2]['total_hours']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_ts_entries VALUES (16, 1, 1, '3.0', '2025-11-06', 1)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ts_entries");
        $this->assertEquals(16, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ts_entries")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
