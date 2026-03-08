<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a loyalty points system through ZTD shadow store (MySQLi).
 * Covers running balance, tier promotion, earn/redeem interleaving,
 * HAVING-based qualification, window functions, and physical isolation.
 * @spec SPEC-10.2.59
 */
class LoyaltyPointsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_lp_members (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                tier VARCHAR(20),
                joined_at DATE
            )',
            'CREATE TABLE mi_lp_point_txns (
                id INT PRIMARY KEY,
                member_id INT,
                points INT,
                txn_type VARCHAR(20),
                description VARCHAR(255),
                created_at DATETIME
            )',
            'CREATE TABLE mi_lp_rewards (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                points_required INT,
                tier_required VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_lp_rewards', 'mi_lp_point_txns', 'mi_lp_members'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_lp_members VALUES (1, 'Alice', 'alice@example.com', 'bronze', '2025-01-01')");
        $this->mysqli->query("INSERT INTO mi_lp_members VALUES (2, 'Bob', 'bob@example.com', 'silver', '2025-03-15')");
        $this->mysqli->query("INSERT INTO mi_lp_members VALUES (3, 'Charlie', 'charlie@example.com', 'gold', '2024-06-01')");
        $this->mysqli->query("INSERT INTO mi_lp_members VALUES (4, 'Diana', 'diana@example.com', 'bronze', '2025-11-01')");

        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (1, 1, 100, 'earn', 'Welcome bonus', '2025-01-01 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (2, 1, 200, 'earn', 'Purchase #101', '2025-02-15 14:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (3, 1, -50, 'redeem', 'Free coffee', '2025-03-01 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (4, 2, 500, 'earn', 'Sign-up promo', '2025-03-15 12:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (5, 2, 300, 'earn', 'Purchase #202', '2025-04-10 16:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (6, 2, -200, 'redeem', 'Gift card', '2025-05-01 11:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (7, 3, 1000, 'earn', 'Loyalty migration', '2024-06-01 08:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (8, 3, 500, 'earn', 'Annual bonus', '2025-01-01 00:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (9, 3, 250, 'earn', 'Referral bonus', '2025-06-15 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (10, 4, 50, 'earn', 'Welcome bonus', '2025-11-01 10:00:00')");

        $this->mysqli->query("INSERT INTO mi_lp_rewards VALUES (1, 'Free Coffee', 100, 'bronze')");
        $this->mysqli->query("INSERT INTO mi_lp_rewards VALUES (2, 'Movie Ticket', 300, 'bronze')");
        $this->mysqli->query("INSERT INTO mi_lp_rewards VALUES (3, 'Spa Voucher', 500, 'silver')");
        $this->mysqli->query("INSERT INTO mi_lp_rewards VALUES (4, 'Weekend Getaway', 1500, 'gold')");
    }

    public function testMemberBalances(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, SUM(pt.points) AS balance
             FROM mi_lp_members m
             JOIN mi_lp_point_txns pt ON pt.member_id = m.id
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

    public function testTierCalculation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, SUM(pt.points) AS balance,
                    CASE
                        WHEN SUM(pt.points) >= 1000 THEN 'gold'
                        WHEN SUM(pt.points) >= 500 THEN 'silver'
                        ELSE 'bronze'
                    END AS calculated_tier
             FROM mi_lp_members m
             JOIN mi_lp_point_txns pt ON pt.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY balance DESC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('gold', $rows[0]['calculated_tier']);
        $this->assertSame('silver', $rows[1]['calculated_tier']);
        $this->assertSame('bronze', $rows[2]['calculated_tier']);
        $this->assertSame('bronze', $rows[3]['calculated_tier']);
    }

    public function testTierPromotion(): void
    {
        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (11, 1, 300, 'earn', 'Big purchase', '2025-07-01 12:00:00')");

        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM mi_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(550, (int) $rows[0]['balance']);

        $this->mysqli->query("UPDATE mi_lp_members SET tier = 'silver' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT tier FROM mi_lp_members WHERE id = 1");
        $this->assertSame('silver', $rows[0]['tier']);
    }

    public function testEarnRedeemInterleaving(): void
    {
        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM mi_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(250, (int) $rows[0]['balance']);

        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (11, 1, 100, 'earn', 'Purchase #102', '2025-07-01 10:00:00')");
        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM mi_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(350, (int) $rows[0]['balance']);

        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (12, 1, -150, 'redeem', 'Movie + snacks', '2025-07-02 14:00:00')");
        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM mi_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(200, (int) $rows[0]['balance']);

        $this->mysqli->query("INSERT INTO mi_lp_point_txns VALUES (13, 1, 500, 'earn', 'Premium purchase', '2025-07-03 09:00:00')");
        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM mi_lp_point_txns WHERE member_id = 1");
        $this->assertEquals(700, (int) $rows[0]['balance']);
    }

    public function testMembersQualifyingForReward(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, m.tier, SUM(pt.points) AS balance
             FROM mi_lp_members m
             JOIN mi_lp_point_txns pt ON pt.member_id = m.id
             WHERE m.tier IN ('silver', 'gold')
             GROUP BY m.id, m.name, m.tier
             HAVING SUM(pt.points) >= 500
             ORDER BY balance DESC"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testRunningTotalWithWindowFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT pt.id, pt.member_id, pt.points, pt.description,
                    SUM(pt.points) OVER (PARTITION BY pt.member_id ORDER BY pt.created_at) AS running_total
             FROM mi_lp_point_txns pt
             WHERE pt.member_id = 1
             ORDER BY pt.created_at"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(100, (int) $rows[0]['running_total']);
        $this->assertEquals(300, (int) $rows[1]['running_total']);
        $this->assertEquals(250, (int) $rows[2]['running_total']);
    }

    public function testEarnVsRedeemSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name,
                    SUM(CASE WHEN pt.points > 0 THEN pt.points ELSE 0 END) AS total_earned,
                    SUM(CASE WHEN pt.points < 0 THEN ABS(pt.points) ELSE 0 END) AS total_redeemed,
                    SUM(pt.points) AS net_balance
             FROM mi_lp_members m
             JOIN mi_lp_point_txns pt ON pt.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY m.name"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(300, (int) $rows[0]['total_earned']);
        $this->assertEquals(50, (int) $rows[0]['total_redeemed']);
        $this->assertEquals(250, (int) $rows[0]['net_balance']);
    }

    public function testDeleteTransactionAndVerifyBalance(): void
    {
        $this->mysqli->query("DELETE FROM mi_lp_point_txns WHERE id = 6");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery("SELECT SUM(points) AS balance FROM mi_lp_point_txns WHERE member_id = 2");
        $this->assertEquals(800, (int) $rows[0]['balance']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_lp_members VALUES (5, 'Eve', 'eve@example.com', 'bronze', '2026-01-01')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_lp_members");
        $this->assertSame(5, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_lp_members');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
