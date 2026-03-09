<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a coupon/discount system workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers coupon code validation, usage tracking, expiry checking, discount
 * calculations, usage summaries, deactivation, and physical isolation.
 * @spec SPEC-10.2.69
 */
class PostgresCouponDiscountTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cp_coupons (
                id INTEGER PRIMARY KEY,
                code VARCHAR(50),
                discount_pct INTEGER,
                max_uses INTEGER,
                valid_from DATE,
                valid_until DATE,
                is_active SMALLINT
            )',
            'CREATE TABLE pg_cp_uses (
                id INTEGER PRIMARY KEY,
                coupon_id INTEGER,
                customer_name VARCHAR(255),
                used_at TIMESTAMP,
                order_total NUMERIC(10,2),
                discount_amount NUMERIC(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cp_uses', 'pg_cp_coupons'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 coupons
        $this->pdo->exec("INSERT INTO pg_cp_coupons VALUES (1, 'SAVE10', 10, 100, '2026-01-01', '2026-12-31', 1)");
        $this->pdo->exec("INSERT INTO pg_cp_coupons VALUES (2, 'HALF50', 50, 10, '2026-01-01', '2026-06-30', 1)");
        $this->pdo->exec("INSERT INTO pg_cp_coupons VALUES (3, 'EXPIRED20', 20, 50, '2025-01-01', '2025-12-31', 1)");
        $this->pdo->exec("INSERT INTO pg_cp_coupons VALUES (4, 'DISABLED15', 15, 200, '2026-01-01', '2026-12-31', 0)");

        // 5 coupon uses
        $this->pdo->exec("INSERT INTO pg_cp_uses VALUES (1, 1, 'Alice', '2026-02-01 10:00:00', 200.00, 20.00)");
        $this->pdo->exec("INSERT INTO pg_cp_uses VALUES (2, 1, 'Bob', '2026-02-15 14:30:00', 150.00, 15.00)");
        $this->pdo->exec("INSERT INTO pg_cp_uses VALUES (3, 1, 'Charlie', '2026-03-01 09:00:00', 300.00, 30.00)");
        $this->pdo->exec("INSERT INTO pg_cp_uses VALUES (4, 2, 'Diana', '2026-03-05 16:00:00', 500.00, 250.00)");
        $this->pdo->exec("INSERT INTO pg_cp_uses VALUES (5, 2, 'Eve', '2026-03-06 11:00:00', 120.00, 60.00)");
    }

    /**
     * Look up a valid coupon by code using a prepared statement.
     */
    public function testValidCouponLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, code, discount_pct, max_uses, valid_from, valid_until
             FROM pg_cp_coupons
             WHERE code = ? AND is_active = 1",
            ['SAVE10']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('SAVE10', $rows[0]['code']);
        $this->assertEquals(10, (int) $rows[0]['discount_pct']);
        $this->assertEquals(100, (int) $rows[0]['max_uses']);
    }

    /**
     * Check usage count against max_uses via LEFT JOIN and GROUP BY.
     */
    public function testUsageLimitCheck(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.code, c.max_uses, COUNT(u.id) AS use_count
             FROM pg_cp_coupons c
             LEFT JOIN pg_cp_uses u ON u.coupon_id = c.id
             WHERE c.is_active = 1
             GROUP BY c.id, c.code, c.max_uses
             ORDER BY c.id"
        );

        // Active coupons: SAVE10, HALF50, EXPIRED20 (3 rows; DISABLED15 excluded)
        $this->assertCount(3, $rows);

        // SAVE10: 3 uses out of 100
        $this->assertSame('SAVE10', $rows[0]['code']);
        $this->assertEquals(3, (int) $rows[0]['use_count']);
        $this->assertEquals(100, (int) $rows[0]['max_uses']);

        // HALF50: 2 uses out of 10
        $this->assertSame('HALF50', $rows[1]['code']);
        $this->assertEquals(2, (int) $rows[1]['use_count']);
        $this->assertEquals(10, (int) $rows[1]['max_uses']);

        // EXPIRED20: 0 uses out of 50
        $this->assertSame('EXPIRED20', $rows[2]['code']);
        $this->assertEquals(0, (int) $rows[2]['use_count']);
    }

    /**
     * Apply a coupon: insert a usage record and verify discount calculation.
     */
    public function testApplyCoupon(): void
    {
        $orderTotal = 400.00;
        $discountPct = 10;
        $discountAmount = $orderTotal * $discountPct / 100; // 40.00

        $this->pdo->exec("INSERT INTO pg_cp_uses VALUES (6, 1, 'Frank', '2026-03-09 12:00:00', {$orderTotal}, {$discountAmount})");

        $rows = $this->ztdQuery("SELECT order_total, discount_amount FROM pg_cp_uses WHERE id = 6");
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(400.00, (float) $rows[0]['order_total'], 0.01);
        $this->assertEqualsWithDelta(40.00, (float) $rows[0]['discount_amount'], 0.01);

        // Verify the use count increased
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS use_count FROM pg_cp_uses WHERE coupon_id = 1"
        );
        $this->assertEquals(4, (int) $rows[0]['use_count']);
    }

    /**
     * Expired coupons are excluded when filtering by date.
     */
    public function testExpiredCouponExcluded(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, code, valid_until
             FROM pg_cp_coupons
             WHERE is_active = 1
               AND valid_from <= ?
               AND valid_until >= ?
             ORDER BY id",
            ['2026-03-09', '2026-03-09']
        );

        // SAVE10 valid through 2026-12-31: included
        // HALF50 valid through 2026-06-30: included
        // EXPIRED20 valid_until 2025-12-31: excluded
        $this->assertCount(2, $rows);
        $codes = array_column($rows, 'code');
        $this->assertContains('SAVE10', $codes);
        $this->assertContains('HALF50', $codes);
        $this->assertNotContains('EXPIRED20', $codes);
    }

    /**
     * Aggregate usage summary per coupon: count of uses and total discount given.
     */
    public function testUsageSummaryPerCoupon(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.code,
                    COUNT(u.id) AS total_uses,
                    COALESCE(SUM(u.discount_amount), 0) AS total_discount
             FROM pg_cp_coupons c
             LEFT JOIN pg_cp_uses u ON u.coupon_id = c.id
             GROUP BY c.id, c.code
             ORDER BY c.code"
        );

        $this->assertCount(4, $rows);

        // DISABLED15: 0 uses, 0 discount
        $this->assertSame('DISABLED15', $rows[0]['code']);
        $this->assertEquals(0, (int) $rows[0]['total_uses']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[0]['total_discount'], 0.01);

        // EXPIRED20: 0 uses, 0 discount
        $this->assertSame('EXPIRED20', $rows[1]['code']);
        $this->assertEquals(0, (int) $rows[1]['total_uses']);

        // HALF50: 2 uses, 310.00 total discount
        $this->assertSame('HALF50', $rows[2]['code']);
        $this->assertEquals(2, (int) $rows[2]['total_uses']);
        $this->assertEqualsWithDelta(310.00, (float) $rows[2]['total_discount'], 0.01);

        // SAVE10: 3 uses, 65.00 total discount
        $this->assertSame('SAVE10', $rows[3]['code']);
        $this->assertEquals(3, (int) $rows[3]['total_uses']);
        $this->assertEqualsWithDelta(65.00, (float) $rows[3]['total_discount'], 0.01);
    }

    /**
     * Deactivate a coupon and verify it no longer appears in active lookups.
     */
    public function testDeactivateCoupon(): void
    {
        // Verify HALF50 is currently active
        $rows = $this->ztdQuery("SELECT is_active FROM pg_cp_coupons WHERE code = 'HALF50'");
        $this->assertEquals(1, (int) $rows[0]['is_active']);

        // Deactivate
        $affected = $this->pdo->exec("UPDATE pg_cp_coupons SET is_active = 0 WHERE code = 'HALF50'");
        $this->assertSame(1, $affected);

        // Verify excluded from active lookup
        $rows = $this->ztdQuery(
            "SELECT code FROM pg_cp_coupons WHERE is_active = 1 ORDER BY code"
        );
        $codes = array_column($rows, 'code');
        $this->assertNotContains('HALF50', $codes);
        $this->assertContains('SAVE10', $codes);
        $this->assertContains('EXPIRED20', $codes);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_cp_uses VALUES (6, 1, 'NewUser', '2026-03-09 18:00:00', 100.00, 10.00)");
        $this->pdo->exec("UPDATE pg_cp_coupons SET is_active = 0 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_cp_uses");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT is_active FROM pg_cp_coupons WHERE id = 1");
        $this->assertEquals(0, (int) $rows[0]['is_active']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_cp_uses")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
