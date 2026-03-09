<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a membership tier management scenario through ZTD shadow store (SQLite PDO).
 * A loyalty program where members earn tiers based on cumulative purchase spending.
 * Exercises SUM with GROUP BY, CASE for tier eligibility, LEFT JOIN COALESCE
 * for benefits lookup, UPDATE for tier promotion, prepared BETWEEN for
 * purchase history, and physical isolation check.
 * @spec SPEC-10.2.161
 */
class SqliteMembershipTierTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mt_tier_rules (
                id INTEGER PRIMARY KEY,
                tier_name TEXT,
                min_spending TEXT,
                benefit_pct TEXT
            )',
            'CREATE TABLE sl_mt_members (
                id INTEGER PRIMARY KEY,
                name TEXT,
                tier TEXT,
                joined_at TEXT
            )',
            'CREATE TABLE sl_mt_purchases (
                id INTEGER PRIMARY KEY,
                member_id INTEGER,
                amount TEXT,
                category TEXT,
                purchased_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mt_purchases', 'sl_mt_members', 'sl_mt_tier_rules'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 tier rules
        $this->pdo->exec("INSERT INTO sl_mt_tier_rules VALUES (1, 'bronze', '0', '2')");
        $this->pdo->exec("INSERT INTO sl_mt_tier_rules VALUES (2, 'silver', '500', '5')");
        $this->pdo->exec("INSERT INTO sl_mt_tier_rules VALUES (3, 'gold', '1500', '10')");
        $this->pdo->exec("INSERT INTO sl_mt_tier_rules VALUES (4, 'platinum', '5000', '15')");

        // 5 members
        $this->pdo->exec("INSERT INTO sl_mt_members VALUES (1, 'Alice', 'bronze', '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_mt_members VALUES (2, 'Bob', 'silver', '2025-03-20')");
        $this->pdo->exec("INSERT INTO sl_mt_members VALUES (3, 'Carol', 'gold', '2024-06-01')");
        $this->pdo->exec("INSERT INTO sl_mt_members VALUES (4, 'Dave', 'bronze', '2025-11-10')");
        $this->pdo->exec("INSERT INTO sl_mt_members VALUES (5, 'Eve', 'platinum', '2024-01-01')");

        // 12 purchases
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (1, 1, '150.00', 'electronics', '2025-06-01')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (2, 1, '200.00', 'clothing', '2025-08-15')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (3, 1, '300.00', 'electronics', '2025-12-25')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (4, 2, '600.00', 'home', '2025-07-10')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (5, 2, '250.00', 'electronics', '2025-09-20')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (6, 3, '800.00', 'clothing', '2025-02-14')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (7, 3, '1200.00', 'home', '2025-05-01')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (8, 3, '500.00', 'electronics', '2025-11-11')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (9, 5, '3000.00', 'electronics', '2025-03-15')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (10, 5, '2500.00', 'home', '2025-06-30')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (11, 4, '100.00', 'clothing', '2026-01-05')");
        $this->pdo->exec("INSERT INTO sl_mt_purchases VALUES (12, 2, '180.00', 'clothing', '2025-12-01')");
    }

    /**
     * GROUP BY m.name, p.category with SUM(p.amount), ORDER BY m.name, p.category.
     * Expected 11 rows covering all member/category combinations.
     */
    public function testMemberSpendingByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, p.category, SUM(p.amount) AS total
             FROM sl_mt_members m
             JOIN sl_mt_purchases p ON p.member_id = m.id
             GROUP BY m.name, p.category
             ORDER BY m.name, p.category"
        );

        $this->assertCount(11, $rows);

        // Alice/clothing=200, Alice/electronics=450
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('clothing', $rows[0]['category']);
        $this->assertEquals(200.00, (float) $rows[0]['total']);

        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('electronics', $rows[1]['category']);
        $this->assertEquals(450.00, (float) $rows[1]['total']);

        // Bob/clothing=180, Bob/electronics=250, Bob/home=600
        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertSame('clothing', $rows[2]['category']);
        $this->assertEquals(180.00, (float) $rows[2]['total']);

        $this->assertSame('Bob', $rows[3]['name']);
        $this->assertSame('electronics', $rows[3]['category']);
        $this->assertEquals(250.00, (float) $rows[3]['total']);

        $this->assertSame('Bob', $rows[4]['name']);
        $this->assertSame('home', $rows[4]['category']);
        $this->assertEquals(600.00, (float) $rows[4]['total']);

        // Carol/clothing=800, Carol/electronics=500, Carol/home=1200
        $this->assertSame('Carol', $rows[5]['name']);
        $this->assertSame('clothing', $rows[5]['category']);
        $this->assertEquals(800.00, (float) $rows[5]['total']);

        $this->assertSame('Carol', $rows[6]['name']);
        $this->assertSame('electronics', $rows[6]['category']);
        $this->assertEquals(500.00, (float) $rows[6]['total']);

        $this->assertSame('Carol', $rows[7]['name']);
        $this->assertSame('home', $rows[7]['category']);
        $this->assertEquals(1200.00, (float) $rows[7]['total']);

        // Dave/clothing=100
        $this->assertSame('Dave', $rows[8]['name']);
        $this->assertSame('clothing', $rows[8]['category']);
        $this->assertEquals(100.00, (float) $rows[8]['total']);

        // Eve/electronics=3000, Eve/home=2500
        $this->assertSame('Eve', $rows[9]['name']);
        $this->assertSame('electronics', $rows[9]['category']);
        $this->assertEquals(3000.00, (float) $rows[9]['total']);

        $this->assertSame('Eve', $rows[10]['name']);
        $this->assertSame('home', $rows[10]['category']);
        $this->assertEquals(2500.00, (float) $rows[10]['total']);
    }

    /**
     * SUM spending + CASE for tier label.
     * Expected: Eve/5500/platinum, Carol/2500/gold, Bob/1030/silver, Alice/650/silver, Dave/100/bronze.
     */
    public function testTierEligibility(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, SUM(p.amount) AS total_spent,
                    CASE
                        WHEN SUM(p.amount) >= 5000 THEN 'platinum'
                        WHEN SUM(p.amount) >= 1500 THEN 'gold'
                        WHEN SUM(p.amount) >= 500 THEN 'silver'
                        ELSE 'bronze'
                    END AS eligible_tier
             FROM sl_mt_members m
             JOIN sl_mt_purchases p ON p.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY total_spent DESC"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertEquals(5500.00, (float) $rows[0]['total_spent']);
        $this->assertSame('platinum', $rows[0]['eligible_tier']);

        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertEquals(2500.00, (float) $rows[1]['total_spent']);
        $this->assertSame('gold', $rows[1]['eligible_tier']);

        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertEquals(1030.00, (float) $rows[2]['total_spent']);
        $this->assertSame('silver', $rows[2]['eligible_tier']);

        $this->assertSame('Alice', $rows[3]['name']);
        $this->assertEquals(650.00, (float) $rows[3]['total_spent']);
        $this->assertSame('silver', $rows[3]['eligible_tier']);

        $this->assertSame('Dave', $rows[4]['name']);
        $this->assertEquals(100.00, (float) $rows[4]['total_spent']);
        $this->assertSame('bronze', $rows[4]['eligible_tier']);
    }

    /**
     * LEFT JOIN tier_rules COALESCE for benefits lookup.
     * Expected: Alice/bronze/2, Bob/silver/5, Carol/gold/10, Dave/bronze/2, Eve/platinum/15.
     */
    public function testMemberBenefits(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, m.tier, COALESCE(t.benefit_pct, 0) AS benefit_pct
             FROM sl_mt_members m
             LEFT JOIN sl_mt_tier_rules t ON t.tier_name = m.tier
             ORDER BY m.name"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('bronze', $rows[0]['tier']);
        $this->assertEquals(2, (int) $rows[0]['benefit_pct']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('silver', $rows[1]['tier']);
        $this->assertEquals(5, (int) $rows[1]['benefit_pct']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertSame('gold', $rows[2]['tier']);
        $this->assertEquals(10, (int) $rows[2]['benefit_pct']);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertSame('bronze', $rows[3]['tier']);
        $this->assertEquals(2, (int) $rows[3]['benefit_pct']);

        $this->assertSame('Eve', $rows[4]['name']);
        $this->assertSame('platinum', $rows[4]['tier']);
        $this->assertEquals(15, (int) $rows[4]['benefit_pct']);
    }

    /**
     * UPDATE Alice to silver, then SELECT to verify.
     */
    public function testUpgradeTier(): void
    {
        $this->pdo->exec("UPDATE sl_mt_members SET tier = 'silver' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, tier FROM sl_mt_members WHERE id = 1");

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('silver', $rows[0]['tier']);
    }

    /**
     * Prepared BETWEEN + JOIN for purchase history in a date range.
     * Params: ['2025-06-01', '2025-09-30']
     * Expected 5 rows ordered by purchased_at, id.
     */
    public function testPreparedPurchaseHistory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT m.name, p.amount, p.category, p.purchased_at
             FROM sl_mt_purchases p
             JOIN sl_mt_members m ON m.id = p.member_id
             WHERE p.purchased_at BETWEEN ? AND ?
             ORDER BY p.purchased_at, p.id",
            ['2025-06-01', '2025-09-30']
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(150.00, (float) $rows[0]['amount']);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertSame('2025-06-01', $rows[0]['purchased_at']);

        $this->assertSame('Eve', $rows[1]['name']);
        $this->assertEquals(2500.00, (float) $rows[1]['amount']);
        $this->assertSame('home', $rows[1]['category']);
        $this->assertSame('2025-06-30', $rows[1]['purchased_at']);

        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertEquals(600.00, (float) $rows[2]['amount']);
        $this->assertSame('home', $rows[2]['category']);
        $this->assertSame('2025-07-10', $rows[2]['purchased_at']);

        $this->assertSame('Alice', $rows[3]['name']);
        $this->assertEquals(200.00, (float) $rows[3]['amount']);
        $this->assertSame('clothing', $rows[3]['category']);
        $this->assertSame('2025-08-15', $rows[3]['purchased_at']);

        $this->assertSame('Bob', $rows[4]['name']);
        $this->assertEquals(250.00, (float) $rows[4]['amount']);
        $this->assertSame('electronics', $rows[4]['category']);
        $this->assertSame('2025-09-20', $rows[4]['purchased_at']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert new member via shadow
        $this->pdo->exec("INSERT INTO sl_mt_members VALUES (6, 'Frank', 'bronze', '2026-03-01')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_mt_members");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_mt_members")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
