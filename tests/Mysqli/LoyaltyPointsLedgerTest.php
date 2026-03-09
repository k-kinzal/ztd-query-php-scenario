<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests loyalty points ledger with sequential mutations, running balance consistency,
 * and HAVING clause behavior (MySQLi).
 * SQL patterns exercised: GROUP BY with SUM for net points, HAVING filter,
 * sequential INSERT/UPDATE mutations with aggregation verification,
 * COUNT with CASE WHEN conditional aggregation, prepared statement with GROUP BY,
 * DELETE + re-aggregate.
 * @spec SPEC-10.2.174
 */
class LoyaltyPointsLedgerTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_lpl_members (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                tier VARCHAR(20),
                balance INT
            )',
            'CREATE TABLE mi_lpl_points_ledger (
                id INT AUTO_INCREMENT PRIMARY KEY,
                member_id INT,
                txn_type VARCHAR(20),
                points INT,
                description VARCHAR(200)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_lpl_points_ledger', 'mi_lpl_members'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Members
        $this->mysqli->query("INSERT INTO mi_lpl_members VALUES (1, 'Alice', 'gold', 500)");
        $this->mysqli->query("INSERT INTO mi_lpl_members VALUES (2, 'Bob', 'silver', 200)");
        $this->mysqli->query("INSERT INTO mi_lpl_members VALUES (3, 'Carol', 'gold', 1000)");

        // Points ledger
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (1, 1, 'earn', 300, 'Purchase bonus')");
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (2, 1, 'earn', 200, 'Referral reward')");
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (3, 1, 'redeem', -100, 'Gift card')");
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (4, 2, 'earn', 150, 'Purchase bonus')");
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (5, 2, 'earn', 50, 'Survey reward')");
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (6, 3, 'earn', 500, 'Annual bonus')");
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (7, 3, 'redeem', -200, 'Merchandise')");
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (8, 3, 'earn', 700, 'Purchase bonus')");
    }

    /**
     * GROUP BY member with SUM of points.
     * Alice: 300+200-100 = 400, Bob: 150+50 = 200, Carol: 500-200+700 = 1000.
     */
    public function testNetPointsByMember(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, SUM(l.points) AS net_points
             FROM mi_lpl_members m
             JOIN mi_lpl_points_ledger l ON l.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY m.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(400, (int) $rows[0]['net_points']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(200, (int) $rows[1]['net_points']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(1000, (int) $rows[2]['net_points']);
    }

    /**
     * HAVING clause to filter members with net points > 300.
     * Bob (200) excluded; Alice (400) and Carol (1000) remain.
     */
    public function testHavingFilterHighEarners(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, SUM(l.points) AS net_points
             FROM mi_lpl_members m
             JOIN mi_lpl_points_ledger l ON l.member_id = m.id
             GROUP BY m.id, m.name
             HAVING SUM(l.points) > 300
             ORDER BY m.name"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(400, (int) $rows[0]['net_points']);

        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertEquals(1000, (int) $rows[1]['net_points']);
    }

    /**
     * Sequential mutations: INSERT two ledger entries + UPDATE balance, then verify aggregation.
     * Bob starts with ledger net 200, balance 200.
     * After: +500 earn, -100 redeem => ledger net 600; balance 200+400 = 600.
     */
    public function testSequentialMutationsConsistency(): void
    {
        // 3 sequential mutations via ztdExec
        $this->ztdExec("INSERT INTO mi_lpl_points_ledger VALUES (9, 2, 'earn', 500, 'Bonus event')");
        $this->ztdExec("INSERT INTO mi_lpl_points_ledger VALUES (10, 2, 'redeem', -100, 'Coupon used')");
        $this->ztdExec("UPDATE mi_lpl_members SET balance = balance + 400 WHERE id = 2");

        // Verify Bob's ledger net: 150+50+500-100 = 600
        $rows = $this->ztdQuery(
            "SELECT SUM(l.points) AS net_points
             FROM mi_lpl_points_ledger l
             WHERE l.member_id = 2"
        );
        $this->assertEquals(600, (int) $rows[0]['net_points']);

        // Verify Bob's balance: 200+400 = 600
        $rows = $this->ztdQuery(
            "SELECT balance FROM mi_lpl_members WHERE id = 2"
        );
        $this->assertEquals(600, (int) $rows[0]['balance']);
    }

    /**
     * COUNT with CASE WHEN conditional aggregation per member.
     * Alice: 2 earns, 1 redeem, net 400.
     * Bob: 2 earns, 0 redeems, net 200.
     * Carol: 2 earns, 1 redeem, net 1000.
     */
    public function testConditionalCountByTxnType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name,
                    COUNT(CASE WHEN l.txn_type = 'earn' THEN 1 END) AS earn_count,
                    COUNT(CASE WHEN l.txn_type = 'redeem' THEN 1 END) AS redeem_count,
                    SUM(l.points) AS net_points
             FROM mi_lpl_members m
             JOIN mi_lpl_points_ledger l ON l.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY m.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['earn_count']);
        $this->assertEquals(1, (int) $rows[0]['redeem_count']);
        $this->assertEquals(400, (int) $rows[0]['net_points']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['earn_count']);
        $this->assertEquals(0, (int) $rows[1]['redeem_count']);
        $this->assertEquals(200, (int) $rows[1]['net_points']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['earn_count']);
        $this->assertEquals(1, (int) $rows[2]['redeem_count']);
        $this->assertEquals(1000, (int) $rows[2]['net_points']);
    }

    /**
     * Prepared statement: total net points by tier.
     * Gold tier: Alice (400) + Carol (1000) = 1400.
     */
    public function testPreparedLedgerByTier(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT m.tier, SUM(l.points) AS tier_total
             FROM mi_lpl_members m
             JOIN mi_lpl_points_ledger l ON l.member_id = m.id
             WHERE m.tier = ?
             GROUP BY m.tier",
            ['gold']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('gold', $rows[0]['tier']);
        $this->assertEquals(1400, (int) $rows[0]['tier_total']);
    }

    /**
     * DELETE ledger entry and re-aggregate.
     * Remove Carol's 700-point purchase bonus (id=8).
     * Carol's net: 500 - 200 = 300.
     */
    public function testDeleteAndReaggregate(): void
    {
        $affected = $this->ztdExec("DELETE FROM mi_lpl_points_ledger WHERE id = 8");
        $this->assertEquals(1, $affected);

        $rows = $this->ztdQuery(
            "SELECT m.name, SUM(l.points) AS net_points
             FROM mi_lpl_members m
             JOIN mi_lpl_points_ledger l ON l.member_id = m.id
             WHERE m.id = 3
             GROUP BY m.id, m.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertEquals(300, (int) $rows[0]['net_points']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_lpl_points_ledger VALUES (9, 1, 'earn', 100, 'Test')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_lpl_points_ledger");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_lpl_points_ledger');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
