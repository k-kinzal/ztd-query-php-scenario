<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests an email campaign workflow through ZTD shadow store (MySQLi).
 * Covers batch status updates, percentage calculations via CASE,
 * delivery metrics, campaign comparison, and physical isolation.
 * @spec SPEC-10.2.79
 */
class EmailCampaignTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ec_campaigns (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                subject VARCHAR(200),
                status VARCHAR(20),
                sent_at DATETIME
            )',
            'CREATE TABLE mi_ec_recipients (
                id INT PRIMARY KEY,
                campaign_id INT,
                email VARCHAR(200),
                delivery_status VARCHAR(20),
                opened_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ec_recipients', 'mi_ec_campaigns'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 campaigns
        $this->mysqli->query("INSERT INTO mi_ec_campaigns VALUES (1, 'Spring Sale', 'Big Spring Deals!', 'sent', '2026-03-01 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_ec_campaigns VALUES (2, 'Summer Preview', 'Summer is Coming', 'draft', NULL)");

        // 6 recipients for campaign 1
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (1, 1, 'alice@example.com', 'delivered', '2026-03-01 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (2, 1, 'bob@example.com', 'delivered', '2026-03-01 11:30:00')");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (3, 1, 'charlie@example.com', 'delivered', NULL)");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (4, 1, 'diana@example.com', 'bounced', NULL)");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (5, 1, 'eve@example.com', 'pending', NULL)");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (6, 1, 'frank@example.com', 'delivered', '2026-03-02 08:00:00')");
    }

    /**
     * Campaign overview: count recipients and delivered count via CASE.
     */
    public function testCampaignOverview(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name,
                    COUNT(r.id) AS total_recipients,
                    SUM(CASE WHEN r.delivery_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count
             FROM mi_ec_campaigns c
             LEFT JOIN mi_ec_recipients r ON r.campaign_id = c.id
             GROUP BY c.id, c.name
             ORDER BY c.id"
        );

        $this->assertCount(2, $rows);

        // Spring Sale: 6 recipients, 4 delivered
        $this->assertSame('Spring Sale', $rows[0]['name']);
        $this->assertEquals(6, (int) $rows[0]['total_recipients']);
        $this->assertEquals(4, (int) $rows[0]['delivered_count']);

        // Summer Preview: 0 recipients
        $this->assertSame('Summer Preview', $rows[1]['name']);
        $this->assertEquals(0, (int) $rows[1]['total_recipients']);
    }

    /**
     * Send a campaign: update status and batch insert recipients.
     */
    public function testSendCampaign(): void
    {
        $this->mysqli->query("UPDATE mi_ec_campaigns SET status = 'sent', sent_at = '2026-03-09 10:00:00' WHERE id = 2");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (7, 2, 'grace@example.com', 'pending', NULL)");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (8, 2, 'hank@example.com', 'pending', NULL)");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (9, 2, 'ivy@example.com', 'pending', NULL)");

        $rows = $this->ztdQuery("SELECT status, sent_at FROM mi_ec_campaigns WHERE id = 2");
        $this->assertSame('sent', $rows[0]['status']);
        $this->assertNotNull($rows[0]['sent_at']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ec_recipients WHERE campaign_id = 2");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Track delivery: update multiple recipients' delivery status using IN clause.
     */
    public function testTrackDelivery(): void
    {
        $this->mysqli->query("UPDATE mi_ec_recipients SET delivery_status = 'delivered' WHERE id IN (5)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery(
            "SELECT id, delivery_status FROM mi_ec_recipients WHERE id = 5"
        );
        $this->assertSame('delivered', $rows[0]['delivery_status']);

        // Verify total delivered count increased
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_ec_recipients
             WHERE campaign_id = 1 AND delivery_status = 'delivered'"
        );
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * Open rate calculation using CASE to count opened vs total, grouped by campaign.
     */
    public function testOpenRateCalculation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name,
                    COUNT(r.id) AS total,
                    SUM(CASE WHEN r.opened_at IS NOT NULL THEN 1 ELSE 0 END) AS opened_count
             FROM mi_ec_campaigns c
             JOIN mi_ec_recipients r ON r.campaign_id = c.id
             GROUP BY c.id, c.name
             ORDER BY c.id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Spring Sale', $rows[0]['name']);
        $this->assertEquals(6, (int) $rows[0]['total']);
        // 3 opened: alice, bob, frank
        $this->assertEquals(3, (int) $rows[0]['opened_count']);
    }

    /**
     * Bounce handling: mark as bounced and verify count changes.
     */
    public function testBounceHandling(): void
    {
        // Count bounced before
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_ec_recipients
             WHERE campaign_id = 1 AND delivery_status = 'bounced'"
        );
        $this->assertEquals(1, (int) $rows[0]['cnt']);

        // Mark another as bounced
        $this->mysqli->query("UPDATE mi_ec_recipients SET delivery_status = 'bounced' WHERE id = 3");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // Verify bounced count increased
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_ec_recipients
             WHERE campaign_id = 1 AND delivery_status = 'bounced'"
        );
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        // Verify delivered count decreased
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_ec_recipients
             WHERE campaign_id = 1 AND delivery_status = 'delivered'"
        );
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Campaign comparison: two campaigns side by side with LEFT JOIN metrics.
     */
    public function testCampaignComparison(): void
    {
        // Send campaign 2 with some recipients
        $this->mysqli->query("UPDATE mi_ec_campaigns SET status = 'sent', sent_at = '2026-03-09 10:00:00' WHERE id = 2");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (7, 2, 'grace@example.com', 'delivered', '2026-03-09 11:00:00')");
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (8, 2, 'hank@example.com', 'delivered', NULL)");

        $rows = $this->ztdQuery(
            "SELECT c.name, c.status,
                    COUNT(r.id) AS total_recipients,
                    SUM(CASE WHEN r.delivery_status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
                    SUM(CASE WHEN r.opened_at IS NOT NULL THEN 1 ELSE 0 END) AS opened
             FROM mi_ec_campaigns c
             LEFT JOIN mi_ec_recipients r ON r.campaign_id = c.id
             GROUP BY c.id, c.name, c.status
             ORDER BY c.id"
        );

        $this->assertCount(2, $rows);

        // Spring Sale
        $this->assertSame('Spring Sale', $rows[0]['name']);
        $this->assertEquals(6, (int) $rows[0]['total_recipients']);
        $this->assertEquals(4, (int) $rows[0]['delivered']);
        $this->assertEquals(3, (int) $rows[0]['opened']);

        // Summer Preview
        $this->assertSame('Summer Preview', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['total_recipients']);
        $this->assertEquals(2, (int) $rows[1]['delivered']);
        $this->assertEquals(1, (int) $rows[1]['opened']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ec_recipients VALUES (7, 1, 'new@example.com', 'pending', NULL)");
        $this->mysqli->query("UPDATE mi_ec_campaigns SET status = 'archived' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ec_recipients");
        $this->assertSame(7, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM mi_ec_campaigns WHERE id = 1");
        $this->assertSame('archived', $rows[0]['status']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ec_recipients');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
