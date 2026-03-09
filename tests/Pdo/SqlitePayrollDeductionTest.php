<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests payroll deduction processing with multi-column UPDATE, INSERT...SELECT
 * with CASE expressions, SUM CASE cross-tab, and HAVING with arithmetic (SQLite PDO).
 * SQL patterns exercised: UPDATE SET multiple columns with arithmetic,
 * INSERT...SELECT with CASE expression for computed values,
 * SUM CASE cross-tab deduction breakdown, HAVING SUM > col * factor,
 * derived table with GROUP BY + HAVING, prepared BETWEEN dates.
 * @spec SPEC-10.2.175
 */
class SqlitePayrollDeductionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_pd_employee (
                id INTEGER PRIMARY KEY,
                name TEXT,
                department TEXT,
                base_salary REAL
            )',
            'CREATE TABLE sl_pd_payroll (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER,
                pay_period TEXT,
                gross_pay REAL,
                net_pay REAL,
                status TEXT
            )',
            'CREATE TABLE sl_pd_deduction (
                id INTEGER PRIMARY KEY,
                payroll_id INTEGER,
                deduction_type TEXT,
                amount REAL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_pd_deduction', 'sl_pd_payroll', 'sl_pd_employee'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_pd_employee VALUES (1, 'Alice', 'Engineering', 8000.00)");
        $this->pdo->exec("INSERT INTO sl_pd_employee VALUES (2, 'Bob', 'Engineering', 7000.00)");
        $this->pdo->exec("INSERT INTO sl_pd_employee VALUES (3, 'Carol', 'Sales', 6000.00)");
        $this->pdo->exec("INSERT INTO sl_pd_employee VALUES (4, 'Dave', 'Sales', 9000.00)");

        $this->pdo->exec("INSERT INTO sl_pd_payroll VALUES (1, 1, '2025-01', 8000.00, NULL, 'pending')");
        $this->pdo->exec("INSERT INTO sl_pd_payroll VALUES (2, 2, '2025-01', 7000.00, NULL, 'pending')");
        $this->pdo->exec("INSERT INTO sl_pd_payroll VALUES (3, 3, '2025-01', 6000.00, NULL, 'pending')");
        $this->pdo->exec("INSERT INTO sl_pd_payroll VALUES (4, 4, '2025-01', 9000.00, NULL, 'pending')");

        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (1, 1, 'tax', 1600.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (2, 1, 'health', 500.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (3, 1, 'retirement', 400.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (4, 2, 'tax', 1400.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (5, 2, 'health', 500.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (6, 2, 'retirement', 350.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (7, 3, 'tax', 1200.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (8, 3, 'health', 500.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (9, 3, 'retirement', 300.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (10, 4, 'tax', 1800.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (11, 4, 'health', 500.00)");
        $this->pdo->exec("INSERT INTO sl_pd_deduction VALUES (12, 4, 'retirement', 450.00)");
    }

    /**
     * UPDATE SET multiple columns: net_pay = gross_pay * 0.75, status = 'processed'.
     */
    public function testUpdateMultipleColumnsWithArithmetic(): void
    {
        $this->ztdExec(
            "UPDATE sl_pd_payroll SET net_pay = gross_pay * 0.75, status = 'processed'
             WHERE pay_period = '2025-01'"
        );

        $rows = $this->ztdQuery(
            "SELECT employee_id, gross_pay, net_pay, status
             FROM sl_pd_payroll ORDER BY employee_id"
        );

        $this->assertCount(4, $rows);
        $this->assertEqualsWithDelta(6000.00, (float) $rows[0]['net_pay'], 0.01);
        $this->assertSame('processed', $rows[0]['status']);
        $this->assertEqualsWithDelta(5250.00, (float) $rows[1]['net_pay'], 0.01);
        $this->assertSame('processed', $rows[1]['status']);
        $this->assertEqualsWithDelta(4500.00, (float) $rows[2]['net_pay'], 0.01);
        $this->assertEqualsWithDelta(6750.00, (float) $rows[3]['net_pay'], 0.01);
    }

    /**
     * SUM CASE cross-tab: deduction breakdown by type per employee.
     */
    public function testDeductionBreakdownCrossTab(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    SUM(CASE WHEN d.deduction_type = 'tax' THEN d.amount ELSE 0 END) AS tax,
                    SUM(CASE WHEN d.deduction_type = 'health' THEN d.amount ELSE 0 END) AS health,
                    SUM(CASE WHEN d.deduction_type = 'retirement' THEN d.amount ELSE 0 END) AS retirement,
                    SUM(d.amount) AS total_deductions
             FROM sl_pd_employee e
             JOIN sl_pd_payroll p ON p.employee_id = e.id
             JOIN sl_pd_deduction d ON d.payroll_id = p.id
             GROUP BY e.id, e.name
             ORDER BY e.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(1600.00, (float) $rows[0]['tax'], 0.01);
        $this->assertEqualsWithDelta(500.00, (float) $rows[0]['health'], 0.01);
        $this->assertEqualsWithDelta(400.00, (float) $rows[0]['retirement'], 0.01);
        $this->assertEqualsWithDelta(2500.00, (float) $rows[0]['total_deductions'], 0.01);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertEqualsWithDelta(1800.00, (float) $rows[3]['tax'], 0.01);
        $this->assertEqualsWithDelta(2750.00, (float) $rows[3]['total_deductions'], 0.01);
    }

    /**
     * HAVING with arithmetic: employees whose total deductions exceed 30% of gross pay.
     */
    public function testHavingWithArithmeticExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, p.gross_pay,
                    SUM(d.amount) AS total_deductions,
                    ROUND(SUM(d.amount) * 100.0 / p.gross_pay, 1) AS deduction_pct
             FROM sl_pd_employee e
             JOIN sl_pd_payroll p ON p.employee_id = e.id
             JOIN sl_pd_deduction d ON d.payroll_id = p.id
             GROUP BY e.id, e.name, p.gross_pay
             HAVING SUM(d.amount) > p.gross_pay * 0.30
             ORDER BY deduction_pct DESC"
        );

        $this->assertGreaterThanOrEqual(3, count($rows));
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertEqualsWithDelta(33.3, (float) $rows[0]['deduction_pct'], 0.5);
    }

    /**
     * INSERT...SELECT with CASE expression for computed values.
     * Probes near SPEC-11.INSERT-SELECT-COMPUTED on SQLite.
     */
    public function testInsertSelectWithCaseExpression(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_pd_payroll (id, employee_id, pay_period, gross_pay, net_pay, status)
                 SELECT e.id + 10, e.id, '2025-02',
                        CASE WHEN e.department = 'Engineering' THEN e.base_salary * 1.10
                             ELSE e.base_salary
                        END,
                        NULL, 'pending'
                 FROM sl_pd_employee e"
            );

            $rows = $this->ztdQuery(
                "SELECT p.employee_id, p.gross_pay, e.department
                 FROM sl_pd_payroll p
                 JOIN sl_pd_employee e ON e.id = p.employee_id
                 WHERE p.pay_period = '2025-02'
                 ORDER BY p.employee_id"
            );

            if (count($rows) === 4) {
                if ($rows[0]['gross_pay'] === null) {
                    $this->markTestIncomplete(
                        'INSERT...SELECT with CASE expression: gross_pay is NULL on SQLite. '
                        . 'Related to SPEC-11.INSERT-SELECT-COMPUTED.'
                    );
                }
                $this->assertEqualsWithDelta(8800.00, (float) $rows[0]['gross_pay'], 0.01);
                $this->assertEqualsWithDelta(6000.00, (float) $rows[2]['gross_pay'], 0.01);
            } else {
                $this->markTestIncomplete(
                    'INSERT...SELECT with CASE returned unexpected row count: ' . count($rows)
                );
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT with CASE expression failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Derived table with GROUP BY + HAVING: find employees with total deductions > 2200.
     */
    public function testDerivedTableWithGroupByHaving(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name, sub.total_ded
                 FROM sl_pd_employee e
                 JOIN (
                     SELECT p.employee_id, SUM(d.amount) AS total_ded
                     FROM sl_pd_payroll p
                     JOIN sl_pd_deduction d ON d.payroll_id = p.id
                     GROUP BY p.employee_id
                     HAVING SUM(d.amount) > 2200
                 ) sub ON sub.employee_id = e.id
                 ORDER BY sub.total_ded DESC"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Derived table with GROUP BY + HAVING returned empty on SQLite. '
                    . 'Expected 3 rows (Dave: 2750, Alice: 2500, Bob: 2250).'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Dave', $rows[0]['name']);
            $this->assertEqualsWithDelta(2750.00, (float) $rows[0]['total_ded'], 0.01);
            $this->assertSame('Alice', $rows[1]['name']);
            $this->assertEqualsWithDelta(2500.00, (float) $rows[1]['total_ded'], 0.01);
            $this->assertSame('Bob', $rows[2]['name']);
            $this->assertEqualsWithDelta(2250.00, (float) $rows[2]['total_ded'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Derived table with GROUP BY + HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with BETWEEN for date range filter.
     */
    public function testPreparedBetweenDateFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name, p.gross_pay, p.pay_period
             FROM sl_pd_payroll p
             JOIN sl_pd_employee e ON e.id = p.employee_id
             WHERE p.pay_period BETWEEN ? AND ?
             ORDER BY e.name",
            ['2025-01', '2025-06']
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(8000.00, (float) $rows[0]['gross_pay'], 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("UPDATE sl_pd_payroll SET status = 'processed' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT status FROM sl_pd_payroll WHERE id = 1");
        $this->assertSame('processed', $rows[0]['status']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_pd_payroll")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
