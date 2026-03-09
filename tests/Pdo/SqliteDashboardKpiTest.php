<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a dashboard KPI compilation scenario: multiple independent aggregations,
 * cross-entity summaries, and period-filtered metrics (SQLite PDO).
 * @spec SPEC-10.2.112
 */
class SqliteDashboardKpiTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dk_customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                segment TEXT,
                created_at TEXT
            )',
            'CREATE TABLE sl_dk_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                total REAL,
                status TEXT,
                order_date TEXT
            )',
            'CREATE TABLE sl_dk_support_tickets (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                priority TEXT,
                status TEXT,
                created_at TEXT,
                resolved_at TEXT
            )',
            'CREATE TABLE sl_dk_page_views (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                page TEXT,
                view_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dk_page_views', 'sl_dk_support_tickets', 'sl_dk_orders', 'sl_dk_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Customers (5: 3 enterprise, 2 startup)
        $this->pdo->exec("INSERT INTO sl_dk_customers VALUES (1, 'Acme Corp', 'enterprise', '2025-06-01')");
        $this->pdo->exec("INSERT INTO sl_dk_customers VALUES (2, 'Beta Inc', 'startup', '2025-09-15')");
        $this->pdo->exec("INSERT INTO sl_dk_customers VALUES (3, 'Gamma LLC', 'enterprise', '2026-01-10')");
        $this->pdo->exec("INSERT INTO sl_dk_customers VALUES (4, 'Delta Co', 'startup', '2025-11-20')");
        $this->pdo->exec("INSERT INTO sl_dk_customers VALUES (5, 'Epsilon Ltd', 'enterprise', '2026-02-01')");

        // Orders (10: mix of completed/pending/refunded, dates in 2026-02 and 2026-03)
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (1,  1, 250.00, 'completed', '2026-02-05')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (2,  1, 180.00, 'completed', '2026-03-01')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (3,  2, 120.00, 'completed', '2026-02-10')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (4,  2,  90.00, 'pending',   '2026-03-05')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (5,  3, 310.00, 'completed', '2026-02-15')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (6,  3, 200.00, 'refunded',  '2026-03-02')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (7,  4,  75.00, 'completed', '2026-02-20')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (8,  4,  60.00, 'completed', '2026-03-08')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (9,  5, 400.00, 'completed', '2026-02-25')");
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (10, 5, 150.00, 'pending',   '2026-03-10')");

        // Support tickets (6: mix of priorities and statuses)
        $this->pdo->exec("INSERT INTO sl_dk_support_tickets VALUES (1, 1, 'high',   'open',     '2026-02-06', NULL)");
        $this->pdo->exec("INSERT INTO sl_dk_support_tickets VALUES (2, 2, 'medium', 'resolved', '2026-02-11', '2026-02-13')");
        $this->pdo->exec("INSERT INTO sl_dk_support_tickets VALUES (3, 3, 'low',    'open',     '2026-02-16', NULL)");
        $this->pdo->exec("INSERT INTO sl_dk_support_tickets VALUES (4, 4, 'high',   'open',     '2026-03-01', NULL)");
        $this->pdo->exec("INSERT INTO sl_dk_support_tickets VALUES (5, 5, 'medium', 'open',     '2026-03-03', NULL)");
        $this->pdo->exec("INSERT INTO sl_dk_support_tickets VALUES (6, 1, 'low',    'resolved', '2026-03-05', '2026-03-06')");

        // Page views (12)
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (1,  1,    '/home',    '2026-02-05')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (2,  2,    '/pricing', '2026-02-06')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (3,  NULL, '/home',    '2026-02-07')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (4,  3,    '/docs',    '2026-02-08')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (5,  1,    '/pricing', '2026-02-10')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (6,  NULL, '/home',    '2026-02-12')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (7,  4,    '/home',    '2026-02-15')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (8,  2,    '/docs',    '2026-02-18')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (9,  5,    '/pricing', '2026-02-20')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (10, NULL, '/home',    '2026-03-01')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (11, 3,    '/pricing', '2026-03-02')");
        $this->pdo->exec("INSERT INTO sl_dk_page_views VALUES (12, 1,    '/blog',    '2026-03-05')");
    }

    /**
     * Revenue summary: total, count, and average for completed orders.
     */
    public function testRevenueSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(total) AS total_revenue,
                    COUNT(*) AS order_count,
                    AVG(total) AS avg_order
             FROM sl_dk_orders
             WHERE status = 'completed'"
        );

        $this->assertCount(1, $rows);
        // Completed: 250+180+120+310+75+60+400 = 1395
        $this->assertEqualsWithDelta(1395.0, (float) $rows[0]['total_revenue'], 0.01);
        $this->assertEquals(7, (int) $rows[0]['order_count']);
        $this->assertEqualsWithDelta(199.29, (float) $rows[0]['avg_order'], 0.01);
    }

    /**
     * Revenue grouped by customer segment (enterprise vs startup).
     */
    public function testRevenueBySegment(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.segment,
                    SUM(o.total) AS segment_revenue
             FROM sl_dk_customers c
             JOIN sl_dk_orders o ON o.customer_id = c.id
             WHERE o.status = 'completed'
             GROUP BY c.segment
             ORDER BY segment_revenue DESC"
        );

        $this->assertCount(2, $rows);
        // Enterprise: 250+180+310+400 = 1140
        $this->assertSame('enterprise', $rows[0]['segment']);
        $this->assertEqualsWithDelta(1140.0, (float) $rows[0]['segment_revenue'], 0.01);
        // Startup: 120+75+60 = 255
        $this->assertSame('startup', $rows[1]['segment']);
        $this->assertEqualsWithDelta(255.0, (float) $rows[1]['segment_revenue'], 0.01);
    }

    /**
     * Open support tickets grouped by priority.
     */
    public function testOpenTicketsByPriority(): void
    {
        $rows = $this->ztdQuery(
            "SELECT priority,
                    COUNT(*) AS ticket_count
             FROM sl_dk_support_tickets
             WHERE status = 'open'
             GROUP BY priority
             ORDER BY ticket_count DESC, priority"
        );

        // open tickets: high=2, medium=1, low=1
        $this->assertCount(3, $rows);
        $this->assertSame('high', $rows[0]['priority']);
        $this->assertEquals(2, (int) $rows[0]['ticket_count']);
    }

    /**
     * Per-customer health score: completed order count, total revenue, open ticket count.
     */
    public function testCustomerHealthScore(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id,
                    c.name,
                    COUNT(DISTINCT o.id) AS order_count,
                    COALESCE(SUM(DISTINCT o.total), 0) AS total_revenue,
                    COUNT(DISTINCT t.id) AS open_tickets
             FROM sl_dk_customers c
             LEFT JOIN sl_dk_orders o ON o.customer_id = c.id AND o.status = 'completed'
             LEFT JOIN sl_dk_support_tickets t ON t.customer_id = c.id AND t.status = 'open'
             GROUP BY c.id, c.name
             ORDER BY c.id"
        );

        $this->assertCount(5, $rows);

        // Cust 1 (Acme Corp): 2 completed orders (250+180=430), 1 open ticket
        $this->assertSame('Acme Corp', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['order_count']);
        $this->assertEqualsWithDelta(430.0, (float) $rows[0]['total_revenue'], 0.01);
        $this->assertEquals(1, (int) $rows[0]['open_tickets']);

        // Cust 2 (Beta Inc): 1 completed order (120), 0 open tickets
        $this->assertSame('Beta Inc', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['order_count']);
        $this->assertEqualsWithDelta(120.0, (float) $rows[1]['total_revenue'], 0.01);
        $this->assertEquals(0, (int) $rows[1]['open_tickets']);

        // Cust 5 (Epsilon Ltd): 1 completed order (400), 1 open ticket
        $this->assertSame('Epsilon Ltd', $rows[4]['name']);
        $this->assertEquals(1, (int) $rows[4]['order_count']);
        $this->assertEqualsWithDelta(400.0, (float) $rows[4]['total_revenue'], 0.01);
        $this->assertEquals(1, (int) $rows[4]['open_tickets']);
    }

    /**
     * Monthly order trend: count and sum per month across all statuses.
     */
    public function testMonthlyOrderTrend(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTR(order_date, 1, 7) AS month,
                    COUNT(*) AS order_count,
                    SUM(total) AS month_total
             FROM sl_dk_orders
             GROUP BY SUBSTR(order_date, 1, 7)
             ORDER BY month"
        );

        $this->assertCount(2, $rows);

        // 2026-02: 5 orders, sum=250+120+310+75+400=1155
        $this->assertSame('2026-02', $rows[0]['month']);
        $this->assertEquals(5, (int) $rows[0]['order_count']);
        $this->assertEqualsWithDelta(1155.0, (float) $rows[0]['month_total'], 0.01);

        // 2026-03: 5 orders, sum=180+90+200+60+150=680
        $this->assertSame('2026-03', $rows[1]['month']);
        $this->assertEquals(5, (int) $rows[1]['order_count']);
        $this->assertEqualsWithDelta(680.0, (float) $rows[1]['month_total'], 0.01);
    }

    /**
     * Top pages by view count, limited to top 3.
     */
    public function testPageViewsTopPages(): void
    {
        $rows = $this->ztdQuery(
            "SELECT page,
                    COUNT(*) AS views
             FROM sl_dk_page_views
             GROUP BY page
             ORDER BY views DESC
             LIMIT 3"
        );

        $this->assertCount(3, $rows);
        // /home=5, /pricing=4, /docs=2
        $this->assertSame('/home', $rows[0]['page']);
        $this->assertEquals(5, (int) $rows[0]['views']);
        $this->assertSame('/pricing', $rows[1]['page']);
        $this->assertEquals(4, (int) $rows[1]['views']);
        $this->assertSame('/docs', $rows[2]['page']);
        $this->assertEquals(2, (int) $rows[2]['views']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_dk_orders VALUES (11, 1, 999.00, 'completed', '2026-03-15')");
        $this->pdo->exec("UPDATE sl_dk_customers SET segment = 'vip' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dk_orders");
        $this->assertEquals(11, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT segment FROM sl_dk_customers WHERE id = 1");
        $this->assertSame('vip', $rows[0]['segment']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_dk_orders")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
