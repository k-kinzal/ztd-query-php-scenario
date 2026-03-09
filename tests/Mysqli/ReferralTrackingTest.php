<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a referral/affiliate tracking system through ZTD shadow store (MySQLi).
 * Covers self-referencing user table for referral chains, multi-level attribution,
 * commission calculation via self-JOINs and aggregation, and physical isolation.
 * @spec SPEC-10.2.109
 */
class ReferralTrackingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rt_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255),
                email VARCHAR(255),
                referred_by INT NULL,
                signup_date DATE
            )',
            'CREATE TABLE mi_rt_purchases (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                amount DECIMAL(10,2),
                purchase_date DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rt_purchases', 'mi_rt_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users forming a referral tree:
        // alice(1) -> bob(2), charlie(3)
        // bob(2) -> diana(4), eve(5)
        // charlie(3) -> frank(6)
        // diana(4) -> grace(7)
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (1, 'alice', 'alice@example.com', NULL, '2026-01-01')");
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (2, 'bob', 'bob@example.com', 1, '2026-01-05')");
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (3, 'charlie', 'charlie@example.com', 1, '2026-01-10')");
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (4, 'diana', 'diana@example.com', 2, '2026-01-15')");
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (5, 'eve', 'eve@example.com', 2, '2026-01-20')");
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (6, 'frank', 'frank@example.com', 3, '2026-01-25')");
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (7, 'grace', 'grace@example.com', 4, '2026-02-01')");

        // Purchases across users
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (1, 2, 150.00, '2026-02-01')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (2, 3, 200.00, '2026-02-05')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (3, 4, 75.00, '2026-02-10')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (4, 4, 125.00, '2026-02-15')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (5, 5, 300.00, '2026-02-20')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (6, 6, 50.00, '2026-02-25')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (7, 7, 100.00, '2026-03-01')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (8, 1, 250.00, '2026-03-05')");
    }

    /**
     * Self-JOIN: count of users each person referred directly.
     */
    public function testDirectReferralCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.id, r.username, COUNT(u.id) AS referral_count
             FROM mi_rt_users r
             LEFT JOIN mi_rt_users u ON u.referred_by = r.id
             GROUP BY r.id, r.username
             ORDER BY referral_count DESC, r.id ASC"
        );

        $this->assertCount(7, $rows);

        // alice referred bob, charlie = 2
        $alice = array_values(array_filter($rows, fn($r) => $r['username'] === 'alice'))[0];
        $this->assertEquals(2, (int) $alice['referral_count']);

        // bob referred diana, eve = 2
        $bob = array_values(array_filter($rows, fn($r) => $r['username'] === 'bob'))[0];
        $this->assertEquals(2, (int) $bob['referral_count']);

        // charlie referred frank = 1
        $charlie = array_values(array_filter($rows, fn($r) => $r['username'] === 'charlie'))[0];
        $this->assertEquals(1, (int) $charlie['referral_count']);

        // diana referred grace = 1
        $diana = array_values(array_filter($rows, fn($r) => $r['username'] === 'diana'))[0];
        $this->assertEquals(1, (int) $diana['referral_count']);

        // eve, frank, grace referred nobody = 0
        $eve = array_values(array_filter($rows, fn($r) => $r['username'] === 'eve'))[0];
        $this->assertEquals(0, (int) $eve['referral_count']);
    }

    /**
     * 2-level self-JOIN: alice's direct + indirect (level 2) referrals.
     */
    public function testReferralChainDepthTwo(): void
    {
        // Level 1: alice's direct referrals
        $direct = $this->ztdPrepareAndExecute(
            "SELECT u.id, u.username
             FROM mi_rt_users u
             WHERE u.referred_by = ?
             ORDER BY u.id",
            [1]
        );
        $this->assertCount(2, $direct);
        $this->assertSame('bob', $direct[0]['username']);
        $this->assertSame('charlie', $direct[1]['username']);

        // Level 2: users referred by alice's direct referrals (2-level self-JOIN)
        $indirect = $this->ztdPrepareAndExecute(
            "SELECT l2.id, l2.username, l1.username AS referred_via
             FROM mi_rt_users l2
             JOIN mi_rt_users l1 ON l2.referred_by = l1.id
             WHERE l1.referred_by = ?
             ORDER BY l2.id",
            [1]
        );
        $this->assertCount(3, $indirect);
        $this->assertSame('diana', $indirect[0]['username']);
        $this->assertSame('eve', $indirect[1]['username']);
        $this->assertSame('frank', $indirect[2]['username']);

        // Combined: all users within 2 levels of alice
        $allReferrals = $this->ztdPrepareAndExecute(
            "SELECT u.id, u.username
             FROM mi_rt_users u
             WHERE u.referred_by = ?
             OR u.referred_by IN (
                SELECT id FROM mi_rt_users WHERE referred_by = ?
             )
             ORDER BY u.id",
            [1, 1]
        );
        $this->assertCount(5, $allReferrals);
    }

    /**
     * JOIN users+purchases, SUM(amount) grouped by referrer.
     */
    public function testReferralRevenueAttribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.id AS referrer_id, r.username AS referrer,
                    SUM(p.amount) AS total_revenue, COUNT(p.id) AS purchase_count
             FROM mi_rt_users r
             JOIN mi_rt_users u ON u.referred_by = r.id
             JOIN mi_rt_purchases p ON p.user_id = u.id
             GROUP BY r.id, r.username
             ORDER BY total_revenue DESC"
        );

        // alice's referrals (bob, charlie) purchased: 150 + 200 = 350
        $alice = array_values(array_filter($rows, fn($r) => $r['referrer'] === 'alice'))[0];
        $this->assertEqualsWithDelta(350.00, (float) $alice['total_revenue'], 0.01);
        $this->assertEquals(2, (int) $alice['purchase_count']);

        // bob's referrals (diana, eve) purchased: 75 + 125 + 300 = 500
        $bob = array_values(array_filter($rows, fn($r) => $r['referrer'] === 'bob'))[0];
        $this->assertEqualsWithDelta(500.00, (float) $bob['total_revenue'], 0.01);
        $this->assertEquals(3, (int) $bob['purchase_count']);

        // charlie's referrals (frank) purchased: 50
        $charlie = array_values(array_filter($rows, fn($r) => $r['referrer'] === 'charlie'))[0];
        $this->assertEqualsWithDelta(50.00, (float) $charlie['total_revenue'], 0.01);

        // diana's referrals (grace) purchased: 100
        $diana = array_values(array_filter($rows, fn($r) => $r['referrer'] === 'diana'))[0];
        $this->assertEqualsWithDelta(100.00, (float) $diana['total_revenue'], 0.01);
    }

    /**
     * JOIN referrer->referred->purchases, HAVING SUM > threshold.
     */
    public function testTopReferrersByPurchaseValue(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.id AS referrer_id, r.username AS referrer,
                    SUM(p.amount) AS total_revenue
             FROM mi_rt_users r
             JOIN mi_rt_users u ON u.referred_by = r.id
             JOIN mi_rt_purchases p ON p.user_id = u.id
             GROUP BY r.id, r.username
             HAVING SUM(p.amount) > 100
             ORDER BY total_revenue DESC"
        );

        // bob: 500, alice: 350 exceed threshold of 100
        // charlie: 50 does not, diana: 100 does not (not > 100)
        $this->assertCount(2, $rows);
        $this->assertSame('bob', $rows[0]['referrer']);
        $this->assertEqualsWithDelta(500.00, (float) $rows[0]['total_revenue'], 0.01);
        $this->assertSame('alice', $rows[1]['referrer']);
        $this->assertEqualsWithDelta(350.00, (float) $rows[1]['total_revenue'], 0.01);
    }

    /**
     * INSERT user with referred_by, INSERT purchase, verify chain.
     */
    public function testAddNewReferralAndVerify(): void
    {
        // Add a new user referred by eve
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (8, 'heidi', 'heidi@example.com', 5, '2026-03-06')");

        // Verify the referral chain: heidi -> eve -> bob -> alice
        $rows = $this->ztdPrepareAndExecute(
            "SELECT u.username, r.username AS referrer
             FROM mi_rt_users u
             JOIN mi_rt_users r ON u.referred_by = r.id
             WHERE u.id = ?",
            [8]
        );
        $this->assertCount(1, $rows);
        $this->assertSame('heidi', $rows[0]['username']);
        $this->assertSame('eve', $rows[0]['referrer']);

        // Add a purchase for heidi
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (9, 8, 175.00, '2026-03-07')");

        // Verify eve now has referral revenue
        $rows = $this->ztdQuery(
            "SELECT r.username AS referrer, SUM(p.amount) AS total_revenue
             FROM mi_rt_users r
             JOIN mi_rt_users u ON u.referred_by = r.id
             JOIN mi_rt_purchases p ON p.user_id = u.id
             WHERE r.id = 5
             GROUP BY r.id, r.username"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('eve', $rows[0]['referrer']);
        $this->assertEqualsWithDelta(175.00, (float) $rows[0]['total_revenue'], 0.01);
    }

    /**
     * LEFT JOIN WHERE referred IS NULL (leaf nodes with no referrals).
     */
    public function testUsersWithNoReferrals(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.id, u.username
             FROM mi_rt_users u
             LEFT JOIN mi_rt_users r ON r.referred_by = u.id
             WHERE r.id IS NULL
             ORDER BY u.id"
        );

        // Leaf nodes: eve(5), frank(6), grace(7) - they referred nobody
        $this->assertCount(3, $rows);
        $this->assertSame('eve', $rows[0]['username']);
        $this->assertSame('frank', $rows[1]['username']);
        $this->assertSame('grace', $rows[2]['username']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Mutate through ZTD
        $this->mysqli->query("INSERT INTO mi_rt_users (id, username, email, referred_by, signup_date) VALUES (8, 'heidi', 'heidi@example.com', 5, '2026-03-06')");
        $this->mysqli->query("INSERT INTO mi_rt_purchases (id, user_id, amount, purchase_date) VALUES (9, 8, 175.00, '2026-03-07')");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rt_users");
        $this->assertEquals(8, (int) $rows[0]['cnt']);
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rt_purchases");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rt_users');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
