<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests access log sessionization through ZTD shadow store (SQLite PDO).
 * Covers window functions (LAG, ROW_NUMBER) for session detection,
 * page visit counting, funnel analysis, and physical isolation.
 * @spec SPEC-10.2.106
 */
class SqliteAccessLogSessionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_al_logs (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                page TEXT,
                action TEXT,
                created_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_al_logs'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // User 1 (Alice): two sessions on different days
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (1,  1, '/home',     'view',  '2026-03-01 09:00:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (2,  1, '/products', 'view',  '2026-03-01 09:05:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (3,  1, '/products/1','view', '2026-03-01 09:08:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (4,  1, '/cart',     'add',   '2026-03-01 09:10:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (5,  1, '/checkout', 'view',  '2026-03-01 09:15:00')");
        // Session gap (next day)
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (6,  1, '/home',     'view',  '2026-03-02 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (7,  1, '/products', 'view',  '2026-03-02 14:03:00')");

        // User 2 (Bob): one session
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (8,  2, '/home',     'view',  '2026-03-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (9,  2, '/products', 'view',  '2026-03-01 10:02:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (10, 2, '/cart',     'add',   '2026-03-01 10:05:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (11, 2, '/checkout', 'view',  '2026-03-01 10:08:00')");
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (12, 2, '/checkout', 'purchase','2026-03-01 10:10:00')");
    }

    /**
     * Page visit frequency: which pages are most visited.
     */
    public function testPageVisitFrequency(): void
    {
        $rows = $this->ztdQuery(
            "SELECT page, COUNT(*) AS visit_count
             FROM sl_al_logs
             WHERE action = 'view'
             GROUP BY page
             ORDER BY visit_count DESC, page ASC"
        );

        $this->assertGreaterThanOrEqual(4, count($rows));
        // /home and /products are visited most
        $this->assertSame('/home', $rows[0]['page']);
        $this->assertEquals(3, (int) $rows[0]['visit_count']);
        $this->assertSame('/products', $rows[1]['page']);
        $this->assertEquals(3, (int) $rows[1]['visit_count']);
    }

    /**
     * Use LAG window function to compute time between page visits per user.
     */
    public function testTimeBetweenVisits(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, page, created_at,
                    LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at) AS prev_visit
             FROM sl_al_logs
             WHERE user_id = ?
             ORDER BY created_at",
            [1]
        );

        $this->assertCount(7, $rows);
        // First visit has no previous
        $this->assertNull($rows[0]['prev_visit']);
        $this->assertSame('/home', $rows[0]['page']);

        // Second visit has previous
        $this->assertSame('2026-03-01 09:00:00', $rows[1]['prev_visit']);
        $this->assertSame('/products', $rows[1]['page']);
    }

    /**
     * Visit sequence numbering using ROW_NUMBER.
     */
    public function testVisitSequence(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, page,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) AS visit_seq
             FROM sl_al_logs
             WHERE user_id = ?
             ORDER BY created_at",
            [2]
        );

        $this->assertCount(5, $rows);
        $this->assertEquals(1, (int) $rows[0]['visit_seq']);
        $this->assertSame('/home', $rows[0]['page']);
        $this->assertEquals(5, (int) $rows[4]['visit_seq']);
        $this->assertSame('/checkout', $rows[4]['page']);
    }

    /**
     * Funnel analysis: how many users reached each step.
     */
    public function testFunnelAnalysis(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                COUNT(DISTINCT CASE WHEN page = '/home' THEN user_id END) AS step1_home,
                COUNT(DISTINCT CASE WHEN page = '/products' THEN user_id END) AS step2_products,
                COUNT(DISTINCT CASE WHEN page = '/cart' THEN user_id END) AS step3_cart,
                COUNT(DISTINCT CASE WHEN page = '/checkout' THEN user_id END) AS step4_checkout,
                COUNT(DISTINCT CASE WHEN action = 'purchase' THEN user_id END) AS step5_purchase
             FROM sl_al_logs"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['step1_home']);     // alice, bob
        $this->assertEquals(2, (int) $rows[0]['step2_products']); // alice, bob
        $this->assertEquals(2, (int) $rows[0]['step3_cart']);      // alice, bob
        $this->assertEquals(2, (int) $rows[0]['step4_checkout']);  // alice, bob
        $this->assertEquals(1, (int) $rows[0]['step5_purchase']);  // bob only
    }

    /**
     * Log a new page visit and verify it appears in queries.
     */
    public function testLogNewVisit(): void
    {
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (13, 1, '/checkout', 'purchase', '2026-03-02 14:10:00')");

        // Alice now also has a purchase
        $rows = $this->ztdQuery(
            "SELECT COUNT(DISTINCT user_id) AS purchasers FROM sl_al_logs WHERE action = 'purchase'"
        );
        $this->assertEquals(2, (int) $rows[0]['purchasers']);
    }

    /**
     * User activity summary: total visits and unique pages per user.
     */
    public function testUserActivitySummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id,
                    COUNT(*) AS total_actions,
                    COUNT(DISTINCT page) AS unique_pages,
                    MIN(created_at) AS first_visit,
                    MAX(created_at) AS last_visit
             FROM sl_al_logs
             GROUP BY user_id
             ORDER BY user_id"
        );

        $this->assertCount(2, $rows);

        // Alice: 7 actions, pages: home, products, products/1, cart, checkout = 5 unique
        $this->assertEquals(1, (int) $rows[0]['user_id']);
        $this->assertEquals(7, (int) $rows[0]['total_actions']);
        $this->assertEquals(5, (int) $rows[0]['unique_pages']);

        // Bob: 5 actions, pages: home, products, cart, checkout = 4 unique
        $this->assertEquals(2, (int) $rows[1]['user_id']);
        $this->assertEquals(5, (int) $rows[1]['total_actions']);
        $this->assertEquals(4, (int) $rows[1]['unique_pages']);
    }

    /**
     * Daily active users count.
     */
    public function testDailyActiveUsers(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTR(created_at, 1, 10) AS visit_date,
                    COUNT(DISTINCT user_id) AS active_users,
                    COUNT(*) AS total_events
             FROM sl_al_logs
             GROUP BY SUBSTR(created_at, 1, 10)
             ORDER BY visit_date"
        );

        $this->assertCount(2, $rows);
        // 2026-03-01: both users active
        $this->assertSame('2026-03-01', $rows[0]['visit_date']);
        $this->assertEquals(2, (int) $rows[0]['active_users']);
        // 2026-03-02: only alice
        $this->assertSame('2026-03-02', $rows[1]['visit_date']);
        $this->assertEquals(1, (int) $rows[1]['active_users']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_al_logs VALUES (14, 3, '/home', 'view', '2026-03-03 10:00:00')");
        $this->pdo->exec("DELETE FROM sl_al_logs WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_al_logs");
        $this->assertEquals(12, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_al_logs")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
