<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a loyalty points system through ZTD shadow store (SQLite PDO).
 * Covers running balance, tier promotion, earn/redeem interleaving,
 * HAVING-based qualification, window functions, and physical isolation.
 * @spec SPEC-10.2.59
 */
class SqliteLoyaltyPointsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_lp_members (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                tier TEXT,
                joined_at TEXT
            )',
            'CREATE TABLE sl_lp_point_txns (
                id INTEGER PRIMARY KEY,
                member_id INTEGER,
                points INTEGER,
                txn_type TEXT,
                description TEXT,
                created_at TEXT
            )',
            'CREATE TABLE sl_lp_rewards (
                id INTEGER PRIMARY KEY,
                name TEXT,
                points_required INTEGER,
                tier_required TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_lp_rewards', 'sl_lp_point_txns', 'sl_lp_members'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_lp_members VALUES (1, 'Alice', 'alice@example.com', 'bronze', '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_lp_members VALUES (2, 'Bob', 'bob@example.com', 'silver', '2025-03-15')");
        $this->pdo->exec("INSERT INTO sl_lp_members VALUES (3, 'Charlie', 'charlie@example.com', 'gold', '2024-06-01')");
        $this->pdo->exec("INSERT INTO sl_lp_members VALUES (4, 'Diana', 'diana@example.com', 'bronze', '2025-11-01')");

        // Alice: 100 + 200 - 50 = 250 points
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (1, 1, 100, 'earn', 'Welcome bonus', '2025-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (2, 1, 200, 'earn', 'Purchase #101', '2025-02-15 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (3, 1, -50, 'redeem', 'Free coffee', '2025-03-01 09:00:00')");

        // Bob: 500 + 300 - 200 = 600 points
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (4, 2, 500, 'earn', 'Sign-up promo', '2025-03-15 12:00:00')");
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (5, 2, 300, 'earn', 'Purchase #202', '2025-04-10 16:00:00')");
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (6, 2, -200, 'redeem', 'Gift card', '2025-05-01 11:00:00')");

        // Charlie: 1000 + 500 + 250 = 1750 points
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (7, 3, 1000, 'earn', 'Loyalty migration', '2024-06-01 08:00:00')");
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (8, 3, 500, 'earn', 'Annual bonus', '2025-01-01 00:00:00')");
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (9, 3, 250, 'earn', 'Referral bonus', '2025-06-15 10:00:00')");

        // Diana: 50 points (new member)
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (10, 4, 50, 'earn', 'Welcome bonus', '2025-11-01 10:00:00')");

        // Rewards catalog
        $this->pdo->exec("INSERT INTO sl_lp_rewards VALUES (1, 'Free Coffee', 100, 'bronze')");
        $this->pdo->exec("INSERT INTO sl_lp_rewards VALUES (2, 'Movie Ticket', 300, 'bronze')");
        $this->pdo->exec("INSERT INTO sl_lp_rewards VALUES (3, 'Spa Voucher', 500, 'silver')");
        $this->pdo->exec("INSERT INTO sl_lp_rewards VALUES (4, 'Weekend Getaway', 1500, 'gold')");
    }

    /**
     * SUM aggregate to calculate member balances.
     */
    public function testMemberBalances(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, SUM(pt.points) AS balance
             FROM sl_lp_members m
             JOIN sl_lp_point_txns pt ON pt.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY balance DESC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertEquals(1750, (int) $rows[0]['balance']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(600, (int) $rows[1]['balance']);
        $this->assertSame('Alice', $rows[2]['name']);
        $this->assertEquals(250, (int) $rows[2]['balance']);
        $this->assertSame('Diana', $rows[3]['name']);
        $this->assertEquals(50, (int) $rows[3]['balance']);
    }

    /**
     * Tier promotion based on total points using CASE expression.
     */
    public function testTierCalculation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, m.tier AS current_tier, SUM(pt.points) AS balance,
                    CASE
                        WHEN SUM(pt.points) >= 1000 THEN 'gold'
                        WHEN SUM(pt.points) >= 500 THEN 'silver'
                        ELSE 'bronze'
                    END AS calculated_tier
             FROM sl_lp_members m
             JOIN sl_lp_point_txns pt ON pt.member_id = m.id
             GROUP BY m.id, m.name, m.tier
             ORDER BY balance DESC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('gold', $rows[0]['calculated_tier']);     // Charlie: 1750
        $this->assertSame('silver', $rows[1]['calculated_tier']);   // Bob: 600
        $this->assertSame('bronze', $rows[2]['calculated_tier']);   // Alice: 250
        $this->assertSame('bronze', $rows[3]['calculated_tier']);   // Diana: 50
    }

    /**
     * Update tier based on calculated tier from aggregate.
     */
    public function testTierPromotion(): void
    {
        // Alice earns enough to reach silver (needs 500 total, has 250)
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (11, 1, 300, 'earn', 'Big purchase', '2025-07-01 12:00:00')");

        // Verify new balance
        $rows = $this->ztdQuery(
            "SELECT SUM(points) AS balance FROM sl_lp_point_txns WHERE member_id = 1"
        );
        $this->assertEquals(550, (int) $rows[0]['balance']);

        // Update tier
        $this->pdo->exec("UPDATE sl_lp_members SET tier = 'silver' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT tier FROM sl_lp_members WHERE id = 1");
        $this->assertSame('silver', $rows[0]['tier']);
    }

    /**
     * Earn/redeem interleaving with balance verification.
     */
    public function testEarnRedeemInterleaving(): void
    {
        // Alice starts with 250 points
        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM sl_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(250, (int) $rows[0]['balance']);

        // Earn 100
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (11, 1, 100, 'earn', 'Purchase #102', '2025-07-01 10:00:00')");

        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM sl_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(350, (int) $rows[0]['balance']);

        // Redeem 150
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (12, 1, -150, 'redeem', 'Movie + snacks', '2025-07-02 14:00:00')");

        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM sl_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(200, (int) $rows[0]['balance']);

        // Earn 500
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (13, 1, 500, 'earn', 'Premium purchase', '2025-07-03 09:00:00')");

        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM sl_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(700, (int) $rows[0]['balance']);

        // Transaction count
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(6, (int) $rows[0]['cnt']);
    }

    /**
     * HAVING to find members qualifying for a specific reward.
     */
    public function testMembersQualifyingForReward(): void
    {
        // Find members with enough points for 'Spa Voucher' (500 points, silver tier)
        $rows = $this->ztdQuery(
            "SELECT m.name, m.tier, SUM(pt.points) AS balance
             FROM sl_lp_members m
             JOIN sl_lp_point_txns pt ON pt.member_id = m.id
             WHERE m.tier IN ('silver', 'gold')
             GROUP BY m.id, m.name, m.tier
             HAVING SUM(pt.points) >= 500
             ORDER BY balance DESC"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Available rewards for a member based on tier and balance.
     */
    public function testAvailableRewardsForMember(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT rw.name AS reward_name, rw.points_required, rw.tier_required
             FROM sl_lp_rewards rw
             WHERE rw.points_required <= (SELECT SUM(pt.points) FROM sl_lp_point_txns pt WHERE pt.member_id = ?)
               AND rw.tier_required IN (
                   SELECT m.tier FROM sl_lp_members m WHERE m.id = ?
               )
             ORDER BY rw.points_required",
            [1, 1]
        );

        // Alice: 250 points, bronze tier -> can afford Free Coffee (100 pts, bronze)
        $this->assertCount(1, $rows);
        $this->assertSame('Free Coffee', $rows[0]['reward_name']);
    }

    /**
     * Window function: running total of points per member.
     */
    public function testRunningTotalWithWindowFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT pt.id, pt.member_id, pt.points, pt.description,
                    SUM(pt.points) OVER (PARTITION BY pt.member_id ORDER BY pt.created_at) AS running_total
             FROM sl_lp_point_txns pt
             WHERE pt.member_id = 1
             ORDER BY pt.created_at"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(100, (int) $rows[0]['running_total']);   // 100
        $this->assertEquals(300, (int) $rows[1]['running_total']);   // 100 + 200
        $this->assertEquals(250, (int) $rows[2]['running_total']);   // 100 + 200 - 50
    }

    /**
     * Summary report: earn vs redeem totals per member.
     */
    public function testEarnVsRedeemSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name,
                    SUM(CASE WHEN pt.points > 0 THEN pt.points ELSE 0 END) AS total_earned,
                    SUM(CASE WHEN pt.points < 0 THEN ABS(pt.points) ELSE 0 END) AS total_redeemed,
                    SUM(pt.points) AS net_balance
             FROM sl_lp_members m
             JOIN sl_lp_point_txns pt ON pt.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY m.name"
        );

        $this->assertCount(4, $rows);

        // Alice: earned 300, redeemed 50, net 250
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(300, (int) $rows[0]['total_earned']);
        $this->assertEquals(50, (int) $rows[0]['total_redeemed']);
        $this->assertEquals(250, (int) $rows[0]['net_balance']);

        // Bob: earned 800, redeemed 200, net 600
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(800, (int) $rows[1]['total_earned']);
        $this->assertEquals(200, (int) $rows[1]['total_redeemed']);
        $this->assertEquals(600, (int) $rows[1]['net_balance']);

        // Charlie: earned 1750, redeemed 0, net 1750
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertEquals(1750, (int) $rows[2]['total_earned']);
        $this->assertEquals(0, (int) $rows[2]['total_redeemed']);
        $this->assertEquals(1750, (int) $rows[2]['net_balance']);
    }

    /**
     * Delete a member's transaction and verify balance changes.
     */
    public function testDeleteTransactionAndVerifyBalance(): void
    {
        // Delete Bob's redemption (id=6, -200 points)
        $affected = $this->pdo->exec("DELETE FROM sl_lp_point_txns WHERE id = 6");
        $this->assertSame(1, $affected);

        // Bob's balance should be 500 + 300 = 800
        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM sl_lp_point_txns WHERE member_id = 2");
        $this->assertEquals(800, (int) $rows[0]['balance']);
    }

    /**
     * Physical isolation verification.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_lp_members VALUES (5, 'Eve', 'eve@example.com', 'bronze', '2026-01-01')");
        $this->pdo->exec("INSERT INTO sl_lp_point_txns VALUES (11, 5, 100, 'earn', 'Welcome', '2026-01-01 10:00:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_lp_members");
        $this->assertSame(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_lp_members")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
