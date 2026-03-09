<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a customer feedback NPS scenario through ZTD shadow store (SQLite PDO).
 * Net Promoter Score calculation from customer surveys exercises CASE for
 * NPS categories (promoter/passive/detractor), ROUND percentage arithmetic,
 * SUM CASE channel breakdown, LEFT JOIN IS NULL anti-join for customers
 * without feedback, prepared BETWEEN for score filtering, and physical isolation.
 * @spec SPEC-10.2.162
 */
class SqliteCustomerNpsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_nps_customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                segment TEXT,
                signup_date TEXT
            )',
            'CREATE TABLE sl_nps_surveys (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                score INTEGER,
                channel TEXT,
                submitted_at TEXT,
                comment TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_nps_surveys', 'sl_nps_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 6 customers
        $this->pdo->exec("INSERT INTO sl_nps_customers VALUES (1, 'Tech Corp', 'enterprise', '2024-01-15')");
        $this->pdo->exec("INSERT INTO sl_nps_customers VALUES (2, 'Shop LLC', 'smb', '2024-06-20')");
        $this->pdo->exec("INSERT INTO sl_nps_customers VALUES (3, 'Dev Studio', 'startup', '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_nps_customers VALUES (4, 'Mega Inc', 'enterprise', '2024-03-01')");
        $this->pdo->exec("INSERT INTO sl_nps_customers VALUES (5, 'Quick Biz', 'smb', '2025-05-15')");
        $this->pdo->exec("INSERT INTO sl_nps_customers VALUES (6, 'Solo App', 'startup', '2025-09-01')");

        // 9 surveys (customer 6 has none)
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (1, 1, 9, 'email', '2025-06-01', 'Great service')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (2, 1, 10, 'web', '2025-12-15', 'Excellent support')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (3, 2, 7, 'email', '2025-07-20', 'Okay experience')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (4, 3, 3, 'web', '2025-08-10', 'Too expensive')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (5, 3, 6, 'email', '2026-01-05', 'Improving but slow')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (6, 4, 8, 'phone', '2025-09-12', 'Decent product')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (7, 4, 9, 'web', '2026-02-01', 'Love the new feature')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (8, 5, 2, 'phone', '2025-10-30', 'Very disappointed')");
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (9, 5, 4, 'email', '2026-02-15', 'Still not great')");
    }

    /**
     * CASE expression for NPS category labels.
     * score >= 9 = promoter, score 7-8 = passive, score <= 6 = detractor.
     * Expected 9 rows with correct category assignments.
     */
    public function testNpsCategories(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.score,
                    CASE
                        WHEN s.score >= 9 THEN 'promoter'
                        WHEN s.score >= 7 THEN 'passive'
                        ELSE 'detractor'
                    END AS category
             FROM sl_nps_surveys s
             ORDER BY s.id"
        );

        $this->assertCount(9, $rows);

        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(9, (int) $rows[0]['score']);
        $this->assertSame('promoter', $rows[0]['category']);

        $this->assertEquals(3, (int) $rows[2]['id']);
        $this->assertEquals(7, (int) $rows[2]['score']);
        $this->assertSame('passive', $rows[2]['category']);

        $this->assertEquals(4, (int) $rows[3]['id']);
        $this->assertEquals(3, (int) $rows[3]['score']);
        $this->assertSame('detractor', $rows[3]['category']);

        $this->assertEquals(8, (int) $rows[7]['id']);
        $this->assertEquals(2, (int) $rows[7]['score']);
        $this->assertSame('detractor', $rows[7]['category']);

        $this->assertEquals(9, (int) $rows[8]['id']);
        $this->assertEquals(4, (int) $rows[8]['score']);
        $this->assertSame('detractor', $rows[8]['category']);
    }

    /**
     * Overall NPS score using ROUND percentage arithmetic with COUNT.
     * NPS = (promoters/total)*100 - (detractors/total)*100 = (3/9)*100 - (4/9)*100 = -11.1.
     * Expected: total=9, promoters=3, detractors=4, nps=-11.1.
     */
    public function testOverallNpsScore(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                 COUNT(*) AS total,
                 SUM(CASE WHEN s.score >= 9 THEN 1 ELSE 0 END) AS promoters,
                 SUM(CASE WHEN s.score <= 6 THEN 1 ELSE 0 END) AS detractors,
                 ROUND(
                     SUM(CASE WHEN s.score >= 9 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
                     - SUM(CASE WHEN s.score <= 6 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                     1
                 ) AS nps
             FROM sl_nps_surveys s"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(9, (int) $rows[0]['total']);
        $this->assertEquals(3, (int) $rows[0]['promoters']);
        $this->assertEquals(4, (int) $rows[0]['detractors']);
        $this->assertEquals(-11.1, round((float) $rows[0]['nps'], 1));
    }

    /**
     * NPS breakdown by channel: GROUP BY channel with SUM CASE.
     * Expected: email=4/1/2, phone=2/0/1, web=3/2/1.
     */
    public function testNpsByChannel(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.channel,
                    COUNT(*) AS responses,
                    SUM(CASE WHEN s.score >= 9 THEN 1 ELSE 0 END) AS promoters,
                    SUM(CASE WHEN s.score <= 6 THEN 1 ELSE 0 END) AS detractors
             FROM sl_nps_surveys s
             GROUP BY s.channel
             ORDER BY s.channel"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('email', $rows[0]['channel']);
        $this->assertEquals(4, (int) $rows[0]['responses']);
        $this->assertEquals(1, (int) $rows[0]['promoters']);
        $this->assertEquals(2, (int) $rows[0]['detractors']);

        $this->assertSame('phone', $rows[1]['channel']);
        $this->assertEquals(2, (int) $rows[1]['responses']);
        $this->assertEquals(0, (int) $rows[1]['promoters']);
        $this->assertEquals(1, (int) $rows[1]['detractors']);

        $this->assertSame('web', $rows[2]['channel']);
        $this->assertEquals(3, (int) $rows[2]['responses']);
        $this->assertEquals(2, (int) $rows[2]['promoters']);
        $this->assertEquals(1, (int) $rows[2]['detractors']);
    }

    /**
     * LEFT JOIN IS NULL anti-join: customers without any survey feedback.
     * Expected 1 row: Solo App, startup.
     */
    public function testCustomersWithoutFeedback(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name, c.segment
             FROM sl_nps_customers c
             LEFT JOIN sl_nps_surveys s ON s.customer_id = c.id
             WHERE s.id IS NULL
             ORDER BY c.id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Solo App', $rows[0]['name']);
        $this->assertSame('startup', $rows[0]['segment']);
    }

    /**
     * Prepared BETWEEN for score filtering with JOIN.
     * Params: [7, 9]. Expected 4 rows: scores 7, 8, 9, 9.
     */
    public function testPreparedScoreRange(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.name, s.score, s.channel
             FROM sl_nps_surveys s
             JOIN sl_nps_customers c ON c.id = s.customer_id
             WHERE s.score BETWEEN ? AND ?
             ORDER BY s.score, s.id",
            [7, 9]
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Shop LLC', $rows[0]['name']);
        $this->assertEquals(7, (int) $rows[0]['score']);
        $this->assertSame('email', $rows[0]['channel']);

        $this->assertSame('Mega Inc', $rows[1]['name']);
        $this->assertEquals(8, (int) $rows[1]['score']);
        $this->assertSame('phone', $rows[1]['channel']);

        $this->assertSame('Tech Corp', $rows[2]['name']);
        $this->assertEquals(9, (int) $rows[2]['score']);
        $this->assertSame('email', $rows[2]['channel']);

        $this->assertSame('Mega Inc', $rows[3]['name']);
        $this->assertEquals(9, (int) $rows[3]['score']);
        $this->assertSame('web', $rows[3]['channel']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert new survey via shadow
        $this->pdo->exec("INSERT INTO sl_nps_surveys VALUES (10, 6, 8, 'web', '2026-03-01', 'First feedback')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_nps_surveys");
        $this->assertEquals(10, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_nps_surveys")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
