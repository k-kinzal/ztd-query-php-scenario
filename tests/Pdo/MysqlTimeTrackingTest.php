<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a time-tracking workflow through ZTD shadow store (MySQL PDO).
 * Covers billable hours aggregation, multi-table JOIN with SUM/GROUP BY,
 * HAVING filters, date-range prepared queries, and physical isolation.
 * @spec SPEC-10.2.80
 */
class MysqlTimeTrackingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_tt_clients (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                rate_per_hour DECIMAL(10,2)
            )',
            'CREATE TABLE mp_tt_projects (
                id INT PRIMARY KEY,
                client_id INT,
                name VARCHAR(255),
                budget_hours DECIMAL(8,2)
            )',
            'CREATE TABLE mp_tt_time_entries (
                id INT PRIMARY KEY,
                project_id INT,
                employee_name VARCHAR(255),
                hours DECIMAL(6,2),
                entry_date DATETIME,
                billable TINYINT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_tt_time_entries', 'mp_tt_projects', 'mp_tt_clients'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_tt_clients VALUES (1, 'Acme Corp', 150.00)");
        $this->pdo->exec("INSERT INTO mp_tt_clients VALUES (2, 'Globex Inc', 200.00)");

        $this->pdo->exec("INSERT INTO mp_tt_projects VALUES (1, 1, 'Website Redesign', 40.00)");
        $this->pdo->exec("INSERT INTO mp_tt_projects VALUES (2, 1, 'Mobile App', 100.00)");
        $this->pdo->exec("INSERT INTO mp_tt_projects VALUES (3, 2, 'Data Migration', 20.00)");

        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (1, 1, 'Alice', 8.00, '2026-03-02 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (2, 1, 'Alice', 6.50, '2026-03-03 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (3, 1, 'Bob', 7.00, '2026-03-02 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (4, 1, 'Bob', 2.00, '2026-03-04 09:00:00', 0)");
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (5, 2, 'Alice', 5.00, '2026-03-02 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (6, 2, 'Charlie', 8.00, '2026-03-03 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (7, 3, 'Charlie', 10.00, '2026-03-02 09:00:00', 1)");
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (8, 3, 'Charlie', 12.00, '2026-03-03 09:00:00', 1)");
    }

    /**
     * SUM billable hours per project with project names via JOIN.
     */
    public function testBillableHoursByProject(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS project_name, SUM(te.hours) AS total_billable
             FROM mp_tt_time_entries te
             JOIN mp_tt_projects p ON p.id = te.project_id
             WHERE te.billable = 1
             GROUP BY te.project_id, p.name
             ORDER BY te.project_id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Website Redesign', $rows[0]['project_name']);
        $this->assertEquals(21.50, (float) $rows[0]['total_billable']);
        $this->assertSame('Mobile App', $rows[1]['project_name']);
        $this->assertEquals(13.00, (float) $rows[1]['total_billable']);
        $this->assertSame('Data Migration', $rows[2]['project_name']);
        $this->assertEquals(22.00, (float) $rows[2]['total_billable']);
    }

    /**
     * 3-table JOIN: SUM(hours * rate_per_hour) grouped by client.
     */
    public function testClientInvoiceSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name AS client_name,
                    SUM(te.hours) AS total_hours,
                    SUM(te.hours * c.rate_per_hour) AS total_cost
             FROM mp_tt_time_entries te
             JOIN mp_tt_projects p ON p.id = te.project_id
             JOIN mp_tt_clients c ON c.id = p.client_id
             WHERE te.billable = 1
             GROUP BY c.id, c.name
             ORDER BY c.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Acme Corp', $rows[0]['client_name']);
        $this->assertEquals(34.50, (float) $rows[0]['total_hours']);
        $this->assertEquals(5175.00, (float) $rows[0]['total_cost']);
        $this->assertSame('Globex Inc', $rows[1]['client_name']);
        $this->assertEquals(22.00, (float) $rows[1]['total_hours']);
        $this->assertEquals(4400.00, (float) $rows[1]['total_cost']);
    }

    /**
     * Find projects where total hours exceed budget_hours via HAVING.
     */
    public function testOverBudgetProjects(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS project_name,
                    p.budget_hours,
                    SUM(te.hours) AS total_hours
             FROM mp_tt_time_entries te
             JOIN mp_tt_projects p ON p.id = te.project_id
             GROUP BY te.project_id, p.name, p.budget_hours
             HAVING SUM(te.hours) > p.budget_hours
             ORDER BY te.project_id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Data Migration', $rows[0]['project_name']);
        $this->assertEquals(20.00, (float) $rows[0]['budget_hours']);
        $this->assertEquals(22.00, (float) $rows[0]['total_hours']);
    }

    /**
     * Prepared: SUM hours per employee within a date range.
     */
    public function testEmployeeWeeklyHours(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT employee_name, SUM(hours) AS weekly_hours
             FROM mp_tt_time_entries
             WHERE entry_date BETWEEN ? AND ?
             GROUP BY employee_name
             ORDER BY employee_name",
            ['2026-03-02 00:00:00', '2026-03-03 23:59:59']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['employee_name']);
        $this->assertEquals(19.50, (float) $rows[0]['weekly_hours']);
        $this->assertSame('Bob', $rows[1]['employee_name']);
        $this->assertEquals(7.00, (float) $rows[1]['weekly_hours']);
        $this->assertSame('Charlie', $rows[2]['employee_name']);
        $this->assertEquals(30.00, (float) $rows[2]['weekly_hours']);
    }

    /**
     * INSERT a new time entry, verify SUM changes.
     */
    public function testAddTimeEntry(): void
    {
        $before = $this->ztdQuery(
            "SELECT SUM(hours) AS total FROM mp_tt_time_entries WHERE project_id = 2 AND billable = 1"
        );
        $this->assertEquals(13.00, (float) $before[0]['total']);

        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (9, 2, 'Diana', 4.00, '2026-03-04 09:00:00', 1)");

        $after = $this->ztdQuery(
            "SELECT SUM(hours) AS total FROM mp_tt_time_entries WHERE project_id = 2 AND billable = 1"
        );
        $this->assertEquals(17.00, (float) $after[0]['total']);
    }

    /**
     * UPDATE billable=0, verify billable SUM decreases.
     */
    public function testMarkNonBillable(): void
    {
        $before = $this->ztdQuery(
            "SELECT SUM(hours) AS total FROM mp_tt_time_entries WHERE project_id = 1 AND billable = 1"
        );
        $this->assertEquals(21.50, (float) $before[0]['total']);

        $affected = $this->pdo->exec("UPDATE mp_tt_time_entries SET billable = 0 WHERE id = 1");
        $this->assertSame(1, $affected);

        $after = $this->ztdQuery(
            "SELECT SUM(hours) AS total FROM mp_tt_time_entries WHERE project_id = 1 AND billable = 1"
        );
        $this->assertEquals(13.50, (float) $after[0]['total']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_tt_time_entries VALUES (9, 1, 'Eve', 3.00, '2026-03-05 09:00:00', 1)");
        $this->pdo->exec("UPDATE mp_tt_time_entries SET billable = 0 WHERE id = 2");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_tt_time_entries");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_tt_time_entries')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
