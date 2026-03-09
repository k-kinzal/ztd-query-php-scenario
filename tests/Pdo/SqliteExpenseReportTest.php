<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests employee expense reporting with multi-level approval, category breakdown,
 * and reimbursement tracking through ZTD shadow store (SQLite PDO).
 * Covers multi-table JOIN, GROUP BY SUM, self-join, HAVING, scalar subquery percentage,
 * and physical isolation.
 * @spec SPEC-10.2.136
 */
class SqliteExpenseReportTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_er_employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                department TEXT,
                manager_id INTEGER
            )',
            'CREATE TABLE sl_er_expense_reports (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER,
                submitted_date TEXT,
                status TEXT,
                total_amount REAL,
                approved_by INTEGER
            )',
            'CREATE TABLE sl_er_expense_items (
                id INTEGER PRIMARY KEY,
                report_id INTEGER,
                category TEXT,
                description TEXT,
                amount REAL,
                receipt_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_er_expense_items', 'sl_er_expense_reports', 'sl_er_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 employees
        $this->pdo->exec("INSERT INTO sl_er_employees VALUES (1, 'Alice', 'Engineering', NULL)");
        $this->pdo->exec("INSERT INTO sl_er_employees VALUES (2, 'Bob', 'Engineering', 1)");
        $this->pdo->exec("INSERT INTO sl_er_employees VALUES (3, 'Carol', 'Sales', 1)");
        $this->pdo->exec("INSERT INTO sl_er_employees VALUES (4, 'Dave', 'Sales', 3)");

        // 4 expense reports
        $this->pdo->exec("INSERT INTO sl_er_expense_reports VALUES (1, 2, '2025-10-01', 'submitted', 450.00, NULL)");
        $this->pdo->exec("INSERT INTO sl_er_expense_reports VALUES (2, 3, '2025-09-15', 'approved', 320.00, 1)");
        $this->pdo->exec("INSERT INTO sl_er_expense_reports VALUES (3, 4, '2025-09-20', 'rejected', 1200.00, NULL)");
        $this->pdo->exec("INSERT INTO sl_er_expense_reports VALUES (4, 2, '2025-08-01', 'reimbursed', 180.00, 1)");

        // 8 expense items
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (1, 1, 'travel', 'Flight to NYC', 200.00, '2025-09-28')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (2, 1, 'meals', 'Client dinner', 150.00, '2025-09-29')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (3, 1, 'supplies', 'Office supplies', 100.00, '2025-09-30')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (4, 2, 'lodging', 'Hotel 3 nights', 250.00, '2025-09-10')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (5, 2, 'meals', 'Team lunch', 70.00, '2025-09-12')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (6, 3, 'travel', 'Conference flight', 800.00, '2025-09-18')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (7, 3, 'lodging', 'Conference hotel', 400.00, '2025-09-18')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (8, 4, 'supplies', 'Printer cartridge', 80.00, '2025-07-25')");
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (9, 4, 'meals', 'Working lunch', 100.00, '2025-07-28')");
    }

    /**
     * JOIN reports with items, GROUP BY report, verify SUM matches total_amount.
     * Tests multi-table JOIN + GROUP BY SUM.
     */
    public function testExpenseReportWithItemTotals(): void
    {
        $rows = $this->ztdQuery(
            "SELECT er.id,
                    er.total_amount,
                    SUM(ei.amount) AS item_total
             FROM sl_er_expense_reports er
             JOIN sl_er_expense_items ei ON ei.report_id = er.id
             GROUP BY er.id, er.total_amount
             ORDER BY er.id"
        );

        $this->assertCount(4, $rows);

        // Report 1: $450
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(450.00, (float) $rows[0]['total_amount']);
        $this->assertEquals(450.00, (float) $rows[0]['item_total']);

        // Report 2: $320
        $this->assertEquals(2, (int) $rows[1]['id']);
        $this->assertEquals(320.00, (float) $rows[1]['total_amount']);
        $this->assertEquals(320.00, (float) $rows[1]['item_total']);

        // Report 3: $1200
        $this->assertEquals(3, (int) $rows[2]['id']);
        $this->assertEquals(1200.00, (float) $rows[2]['total_amount']);
        $this->assertEquals(1200.00, (float) $rows[2]['item_total']);

        // Report 4: $180
        $this->assertEquals(4, (int) $rows[3]['id']);
        $this->assertEquals(180.00, (float) $rows[3]['total_amount']);
        $this->assertEquals(180.00, (float) $rows[3]['item_total']);
    }

    /**
     * JOIN all 3 tables, GROUP BY department + category, SUM amounts.
     * Tests 3-table JOIN + multi-column GROUP BY.
     */
    public function testCategoryBreakdownByDepartment(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.department,
                    ei.category,
                    SUM(ei.amount) AS category_total
             FROM sl_er_employees e
             JOIN sl_er_expense_reports er ON er.employee_id = e.id
             JOIN sl_er_expense_items ei ON ei.report_id = er.id
             GROUP BY e.department, ei.category
             ORDER BY e.department, ei.category"
        );

        $this->assertCount(6, $rows);

        // Engineering / meals: $250
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('meals', $rows[0]['category']);
        $this->assertEquals(250.00, (float) $rows[0]['category_total']);

        // Engineering / supplies: $180
        $this->assertSame('Engineering', $rows[1]['department']);
        $this->assertSame('supplies', $rows[1]['category']);
        $this->assertEquals(180.00, (float) $rows[1]['category_total']);

        // Engineering / travel: $200
        $this->assertSame('Engineering', $rows[2]['department']);
        $this->assertSame('travel', $rows[2]['category']);
        $this->assertEquals(200.00, (float) $rows[2]['category_total']);

        // Sales / lodging: $650
        $this->assertSame('Sales', $rows[3]['department']);
        $this->assertSame('lodging', $rows[3]['category']);
        $this->assertEquals(650.00, (float) $rows[3]['category_total']);

        // Sales / meals: $70
        $this->assertSame('Sales', $rows[4]['department']);
        $this->assertSame('meals', $rows[4]['category']);
        $this->assertEquals(70.00, (float) $rows[4]['category_total']);

        // Sales / travel: $800
        $this->assertSame('Sales', $rows[5]['department']);
        $this->assertSame('travel', $rows[5]['category']);
        $this->assertEquals(800.00, (float) $rows[5]['category_total']);
    }

    /**
     * Find reports WHERE status='submitted' AND employee's manager_id matches.
     * Self-join employees for manager lookup.
     * Tests self-join + WHERE filter.
     */
    public function testPendingApprovalsForManager(): void
    {
        $rows = $this->ztdQuery(
            "SELECT er.id AS report_id,
                    e.name AS employee_name,
                    m.name AS manager_name,
                    er.total_amount
             FROM sl_er_expense_reports er
             JOIN sl_er_employees e ON e.id = er.employee_id
             JOIN sl_er_employees m ON m.id = e.manager_id
             WHERE er.status = 'submitted'
             ORDER BY er.id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['report_id']);
        $this->assertSame('Bob', $rows[0]['employee_name']);
        $this->assertSame('Alice', $rows[0]['manager_name']);
        $this->assertEquals(450.00, (float) $rows[0]['total_amount']);
    }

    /**
     * SUM amounts for reimbursed reports per employee.
     * Tests GROUP BY + SUM + WHERE filter.
     */
    public function testReimbursementSummaryByEmployee(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    SUM(ei.amount) AS reimbursed_total
             FROM sl_er_employees e
             JOIN sl_er_expense_reports er ON er.employee_id = e.id
             JOIN sl_er_expense_items ei ON ei.report_id = er.id
             WHERE er.status = 'reimbursed'
             GROUP BY e.id, e.name
             ORDER BY e.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertEquals(180.00, (float) $rows[0]['reimbursed_total']);
    }

    /**
     * HAVING SUM(amount) > 500 to find reports with high totals.
     * Tests GROUP BY + HAVING with aggregate threshold.
     */
    public function testHighValueReportsAboveThreshold(): void
    {
        $rows = $this->ztdQuery(
            "SELECT er.id,
                    e.name,
                    SUM(ei.amount) AS item_total
             FROM sl_er_expense_reports er
             JOIN sl_er_employees e ON e.id = er.employee_id
             JOIN sl_er_expense_items ei ON ei.report_id = er.id
             GROUP BY er.id, e.name
             HAVING SUM(ei.amount) > 500
             ORDER BY er.id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['id']);
        $this->assertSame('Dave', $rows[0]['name']);
        $this->assertEquals(1200.00, (float) $rows[0]['item_total']);
    }

    /**
     * For approved/reimbursed reports, show each category as percentage of grand total
     * using scalar subquery.
     * Tests ROUND + scalar subquery percentage.
     */
    public function testCategoryPercentageOfTotal(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ei.category,
                    SUM(ei.amount) AS category_total,
                    ROUND(SUM(ei.amount) * 100.0 / (
                        SELECT SUM(ei2.amount)
                        FROM sl_er_expense_items ei2
                        JOIN sl_er_expense_reports er2 ON er2.id = ei2.report_id
                        WHERE er2.status IN ('approved', 'reimbursed')
                    ), 2) AS pct
             FROM sl_er_expense_items ei
             JOIN sl_er_expense_reports er ON er.id = ei.report_id
             WHERE er.status IN ('approved', 'reimbursed')
             GROUP BY ei.category
             ORDER BY ei.category"
        );

        $this->assertCount(3, $rows);

        // lodging: $250 / $500 = 50.00%
        $this->assertSame('lodging', $rows[0]['category']);
        $this->assertEquals(250.00, (float) $rows[0]['category_total']);
        $this->assertEqualsWithDelta(50.00, (float) $rows[0]['pct'], 0.01);

        // meals: $170 / $500 = 34.00%
        $this->assertSame('meals', $rows[1]['category']);
        $this->assertEquals(170.00, (float) $rows[1]['category_total']);
        $this->assertEqualsWithDelta(34.00, (float) $rows[1]['pct'], 0.01);

        // supplies: $80 / $500 = 16.00%
        $this->assertSame('supplies', $rows[2]['category']);
        $this->assertEquals(80.00, (float) $rows[2]['category_total']);
        $this->assertEqualsWithDelta(16.00, (float) $rows[2]['pct'], 0.01);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new expense item
        $this->pdo->exec("INSERT INTO sl_er_expense_items VALUES (10, 1, 'travel', 'Taxi', 45.00, '2025-10-01')");

        // Visible through ZTD: now 10 items
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_er_expense_items");
        $this->assertSame(10, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_er_expense_items")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
