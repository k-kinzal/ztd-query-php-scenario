<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests Budget Allocation with Rollover — exercises cumulative SUM() OVER
 * (PARTITION BY ... ORDER BY ...) window functions, budget variance
 * calculations, and CASE conditional aggregation (MySQL PDO).
 * @spec SPEC-10.2.128
 */
class MysqlBudgetRolloverTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_br_departments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                annual_budget DECIMAL(12,2)
            )',
            'CREATE TABLE mp_br_expenses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                department_id INT,
                expense_month VARCHAR(10),
                category VARCHAR(50),
                amount DECIMAL(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_br_expenses', 'mp_br_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Departments
        $this->pdo->exec("INSERT INTO mp_br_departments VALUES (1, 'Engineering', 120000.00)");
        $this->pdo->exec("INSERT INTO mp_br_departments VALUES (2, 'Marketing', 60000.00)");
        $this->pdo->exec("INSERT INTO mp_br_departments VALUES (3, 'Sales', 90000.00)");

        // Expenses — Engineering (monthly budget = 10000)
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (1, 1, '2025-01', 'cloud', 3000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (2, 1, '2025-01', 'tools', 2000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (3, 1, '2025-02', 'cloud', 3500.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (4, 1, '2025-02', 'tools', 1500.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (5, 1, '2025-02', 'training', 4000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (6, 1, '2025-03', 'cloud', 3000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (7, 1, '2025-03', 'tools', 1000.00)");

        // Expenses — Marketing (monthly budget = 5000)
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (8, 2, '2025-01', 'ads', 4500.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (9, 2, '2025-01', 'events', 1000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (10, 2, '2025-02', 'ads', 3000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (11, 2, '2025-03', 'ads', 5000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (12, 2, '2025-03', 'events', 2000.00)");

        // Expenses — Sales (monthly budget = 7500)
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (13, 3, '2025-01', 'travel', 2000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (14, 3, '2025-01', 'entertainment', 1500.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (15, 3, '2025-02', 'travel', 3000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (16, 3, '2025-02', 'entertainment', 2500.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (17, 3, '2025-02', 'supplies', 500.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (18, 3, '2025-03', 'travel', 4000.00)");
    }

    /**
     * Monthly spending per department — basic GROUP BY with JOIN.
     */
    public function testMonthlySpendingPerDepartment(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name, e.expense_month, SUM(e.amount) AS monthly_total
             FROM mp_br_departments d
             JOIN mp_br_expenses e ON e.department_id = d.id
             GROUP BY d.id, d.name, e.expense_month
             ORDER BY d.name, e.expense_month"
        );

        $this->assertCount(9, $rows);

        // Engineering: Jan=5000, Feb=9000, Mar=4000
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertSame('2025-01', $rows[0]['expense_month']);
        $this->assertEqualsWithDelta(5000.00, (float) $rows[0]['monthly_total'], 0.01);

        $this->assertSame('Engineering', $rows[1]['name']);
        $this->assertSame('2025-02', $rows[1]['expense_month']);
        $this->assertEqualsWithDelta(9000.00, (float) $rows[1]['monthly_total'], 0.01);

        $this->assertSame('Engineering', $rows[2]['name']);
        $this->assertSame('2025-03', $rows[2]['expense_month']);
        $this->assertEqualsWithDelta(4000.00, (float) $rows[2]['monthly_total'], 0.01);

        // Marketing: Jan=5500, Feb=3000, Mar=7000
        $this->assertSame('Marketing', $rows[3]['name']);
        $this->assertSame('2025-01', $rows[3]['expense_month']);
        $this->assertEqualsWithDelta(5500.00, (float) $rows[3]['monthly_total'], 0.01);

        $this->assertSame('Marketing', $rows[4]['name']);
        $this->assertSame('2025-02', $rows[4]['expense_month']);
        $this->assertEqualsWithDelta(3000.00, (float) $rows[4]['monthly_total'], 0.01);

        $this->assertSame('Marketing', $rows[5]['name']);
        $this->assertSame('2025-03', $rows[5]['expense_month']);
        $this->assertEqualsWithDelta(7000.00, (float) $rows[5]['monthly_total'], 0.01);

        // Sales: Jan=3500, Feb=6000, Mar=4000
        $this->assertSame('Sales', $rows[6]['name']);
        $this->assertSame('2025-01', $rows[6]['expense_month']);
        $this->assertEqualsWithDelta(3500.00, (float) $rows[6]['monthly_total'], 0.01);

        $this->assertSame('Sales', $rows[7]['name']);
        $this->assertSame('2025-02', $rows[7]['expense_month']);
        $this->assertEqualsWithDelta(6000.00, (float) $rows[7]['monthly_total'], 0.01);

        $this->assertSame('Sales', $rows[8]['name']);
        $this->assertSame('2025-03', $rows[8]['expense_month']);
        $this->assertEqualsWithDelta(4000.00, (float) $rows[8]['monthly_total'], 0.01);
    }

    /**
     * Cumulative spending using SUM() OVER with ORDER BY — the key
     * window-function pattern under test.
     */
    public function testCumulativeSpending(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name, e.expense_month,
                    SUM(e.amount) AS monthly_total,
                    SUM(SUM(e.amount)) OVER (PARTITION BY d.id ORDER BY e.expense_month) AS cumulative_total
             FROM mp_br_departments d
             JOIN mp_br_expenses e ON e.department_id = d.id
             GROUP BY d.id, d.name, e.expense_month
             ORDER BY d.name, e.expense_month"
        );

        $this->assertCount(9, $rows);

        // Engineering: Jan cumulative=5000, Feb=14000, Mar=18000
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertEqualsWithDelta(5000.00, (float) $rows[0]['cumulative_total'], 0.01);

        $this->assertSame('Engineering', $rows[1]['name']);
        $this->assertEqualsWithDelta(14000.00, (float) $rows[1]['cumulative_total'], 0.01);

        $this->assertSame('Engineering', $rows[2]['name']);
        $this->assertEqualsWithDelta(18000.00, (float) $rows[2]['cumulative_total'], 0.01);

        // Marketing: Jan=5500, Feb=8500, Mar=15500
        $this->assertSame('Marketing', $rows[3]['name']);
        $this->assertEqualsWithDelta(5500.00, (float) $rows[3]['cumulative_total'], 0.01);

        $this->assertSame('Marketing', $rows[4]['name']);
        $this->assertEqualsWithDelta(8500.00, (float) $rows[4]['cumulative_total'], 0.01);

        $this->assertSame('Marketing', $rows[5]['name']);
        $this->assertEqualsWithDelta(15500.00, (float) $rows[5]['cumulative_total'], 0.01);

        // Sales: Jan=3500, Feb=9500, Mar=13500
        $this->assertSame('Sales', $rows[6]['name']);
        $this->assertEqualsWithDelta(3500.00, (float) $rows[6]['cumulative_total'], 0.01);

        $this->assertSame('Sales', $rows[7]['name']);
        $this->assertEqualsWithDelta(9500.00, (float) $rows[7]['cumulative_total'], 0.01);

        $this->assertSame('Sales', $rows[8]['name']);
        $this->assertEqualsWithDelta(13500.00, (float) $rows[8]['cumulative_total'], 0.01);
    }

    /**
     * Budget variance — compare monthly spending to monthly budget (annual/12).
     */
    public function testBudgetVariance(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name, e.expense_month,
                    SUM(e.amount) AS actual,
                    ROUND(d.annual_budget / 12, 2) AS monthly_budget,
                    ROUND(SUM(e.amount) - d.annual_budget / 12, 2) AS variance
             FROM mp_br_departments d
             JOIN mp_br_expenses e ON e.department_id = d.id
             GROUP BY d.id, d.name, d.annual_budget, e.expense_month
             ORDER BY d.name, e.expense_month"
        );

        $this->assertCount(9, $rows);

        // Engineering monthly budget = 10000
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertEqualsWithDelta(10000.00, (float) $rows[0]['monthly_budget'], 0.01);
        $this->assertEqualsWithDelta(-5000.00, (float) $rows[0]['variance'], 0.01); // Jan

        $this->assertEqualsWithDelta(-1000.00, (float) $rows[1]['variance'], 0.01); // Feb

        $this->assertEqualsWithDelta(-6000.00, (float) $rows[2]['variance'], 0.01); // Mar

        // Marketing monthly budget = 5000
        $this->assertSame('Marketing', $rows[3]['name']);
        $this->assertEqualsWithDelta(5000.00, (float) $rows[3]['monthly_budget'], 0.01);
        $this->assertEqualsWithDelta(500.00, (float) $rows[3]['variance'], 0.01);   // Jan

        $this->assertEqualsWithDelta(-2000.00, (float) $rows[4]['variance'], 0.01); // Feb

        $this->assertEqualsWithDelta(2000.00, (float) $rows[5]['variance'], 0.01);  // Mar

        // Sales monthly budget = 7500
        $this->assertSame('Sales', $rows[6]['name']);
        $this->assertEqualsWithDelta(7500.00, (float) $rows[6]['monthly_budget'], 0.01);
        $this->assertEqualsWithDelta(-4000.00, (float) $rows[6]['variance'], 0.01); // Jan

        $this->assertEqualsWithDelta(-1500.00, (float) $rows[7]['variance'], 0.01); // Feb

        $this->assertEqualsWithDelta(-3500.00, (float) $rows[8]['variance'], 0.01); // Mar
    }

    /**
     * Over-budget months — only months where spending exceeded monthly budget.
     */
    public function testOverBudgetMonths(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name, e.expense_month, SUM(e.amount) AS actual
             FROM mp_br_departments d
             JOIN mp_br_expenses e ON e.department_id = d.id
             GROUP BY d.id, d.name, d.annual_budget, e.expense_month
             HAVING SUM(e.amount) > d.annual_budget / 12
             ORDER BY d.name, e.expense_month"
        );

        // Only Marketing Jan (5500 > 5000) and Marketing Mar (7000 > 5000)
        $this->assertCount(2, $rows);

        $this->assertSame('Marketing', $rows[0]['name']);
        $this->assertSame('2025-01', $rows[0]['expense_month']);
        $this->assertEqualsWithDelta(5500.00, (float) $rows[0]['actual'], 0.01);

        $this->assertSame('Marketing', $rows[1]['name']);
        $this->assertSame('2025-03', $rows[1]['expense_month']);
        $this->assertEqualsWithDelta(7000.00, (float) $rows[1]['actual'], 0.01);
    }

    /**
     * Category breakdown — spending by category with percentage of total.
     */
    public function testCategoryBreakdown(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(amount) AS total,
                    COUNT(*) AS transaction_count,
                    ROUND(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mp_br_expenses WHERE 1=1), 1) AS pct_of_total
             FROM mp_br_expenses
             GROUP BY category
             ORDER BY total DESC"
        );

        $this->assertCount(8, $rows);

        // ads: 4500+3000+5000 = 12500, 3 transactions, 26.6%
        $this->assertSame('ads', $rows[0]['category']);
        $this->assertEqualsWithDelta(12500.00, (float) $rows[0]['total'], 0.01);
        $this->assertEquals(3, (int) $rows[0]['transaction_count']);
        $this->assertEqualsWithDelta(26.6, (float) $rows[0]['pct_of_total'], 0.1);

        // cloud: 3000+3500+3000 = 9500
        $this->assertSame('cloud', $rows[1]['category']);
        $this->assertEqualsWithDelta(9500.00, (float) $rows[1]['total'], 0.01);

        // travel: 2000+3000+4000 = 9000
        $this->assertSame('travel', $rows[2]['category']);
        $this->assertEqualsWithDelta(9000.00, (float) $rows[2]['total'], 0.01);

        // tools: 2000+1500+1000 = 4500
        $this->assertSame('tools', $rows[3]['category']);
        $this->assertEqualsWithDelta(4500.00, (float) $rows[3]['total'], 0.01);

        // training: 4000
        $this->assertSame('training', $rows[4]['category']);
        $this->assertEqualsWithDelta(4000.00, (float) $rows[4]['total'], 0.01);

        // entertainment: 1500+2500 = 4000
        $this->assertSame('entertainment', $rows[5]['category']);
        $this->assertEqualsWithDelta(4000.00, (float) $rows[5]['total'], 0.01);

        // events: 1000+2000 = 3000
        $this->assertSame('events', $rows[6]['category']);
        $this->assertEqualsWithDelta(3000.00, (float) $rows[6]['total'], 0.01);

        // supplies: 500
        $this->assertSame('supplies', $rows[7]['category']);
        $this->assertEqualsWithDelta(500.00, (float) $rows[7]['total'], 0.01);
    }

    /**
     * Department ranking — RANK() OVER by total spending.
     */
    public function testDepartmentRanking(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name, SUM(e.amount) AS total_spent, d.annual_budget,
                    RANK() OVER (ORDER BY SUM(e.amount) DESC) AS spend_rank
             FROM mp_br_departments d
             JOIN mp_br_expenses e ON e.department_id = d.id
             GROUP BY d.id, d.name, d.annual_budget
             ORDER BY spend_rank"
        );

        $this->assertCount(3, $rows);

        // Engineering=18000 rank 1
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertEqualsWithDelta(18000.00, (float) $rows[0]['total_spent'], 0.01);
        $this->assertEquals(1, (int) $rows[0]['spend_rank']);

        // Marketing=15500 rank 2
        $this->assertSame('Marketing', $rows[1]['name']);
        $this->assertEqualsWithDelta(15500.00, (float) $rows[1]['total_spent'], 0.01);
        $this->assertEquals(2, (int) $rows[1]['spend_rank']);

        // Sales=13500 rank 3
        $this->assertSame('Sales', $rows[2]['name']);
        $this->assertEqualsWithDelta(13500.00, (float) $rows[2]['total_spent'], 0.01);
        $this->assertEquals(3, (int) $rows[2]['spend_rank']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_br_departments VALUES (4, 'HR', 50000.00)");
        $this->pdo->exec("INSERT INTO mp_br_expenses VALUES (19, 4, '2025-01', 'recruiting', 2000.00)");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_br_departments");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_br_expenses");
        $this->assertEquals(19, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_br_departments")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
