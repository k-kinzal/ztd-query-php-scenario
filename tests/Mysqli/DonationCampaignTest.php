<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests donation campaign tracking with fundraising goals and donor management (MySQLi).
 * SQL patterns exercised: INSERT with explicit column list in non-DDL order,
 * self-referencing UPDATE arithmetic (raised = raised + amount), chained self-ref updates,
 * COUNT(DISTINCT donor_id), COALESCE(SUM, 0) with LEFT JOIN for zero-donation campaigns,
 * ROUND percentage, DELETE + verify, prepared statement for donor lookup.
 * @spec SPEC-10.2.171
 */
class DonationCampaignTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_dc_donor (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200),
                joined_date TEXT
            )',
            'CREATE TABLE mi_dc_campaign (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200),
                goal DECIMAL(12,2),
                raised DECIMAL(12,2),
                status VARCHAR(20),
                start_date TEXT,
                end_date TEXT
            )',
            'CREATE TABLE mi_dc_donation (
                id INT AUTO_INCREMENT PRIMARY KEY,
                campaign_id INT,
                donor_id INT,
                amount DECIMAL(10,2),
                donated_at TEXT,
                note VARCHAR(200)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_dc_donation', 'mi_dc_campaign', 'mi_dc_donor'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Donors
        $this->mysqli->query("INSERT INTO mi_dc_donor VALUES (1, 'Alice', 'alice@example.com', '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_dc_donor VALUES (2, 'Bob', 'bob@example.com', '2024-03-20')");
        $this->mysqli->query("INSERT INTO mi_dc_donor VALUES (3, 'Carol', 'carol@example.com', '2024-06-01')");
        $this->mysqli->query("INSERT INTO mi_dc_donor VALUES (4, 'Dave', 'dave@example.com', '2025-01-10')");

        // Campaigns
        $this->mysqli->query("INSERT INTO mi_dc_campaign VALUES (1, 'School Library Fund', 10000.00, 6500.00, 'active', '2025-01-01', '2025-06-30')");
        $this->mysqli->query("INSERT INTO mi_dc_campaign VALUES (2, 'Community Garden', 5000.00, 5200.00, 'completed', '2025-01-01', '2025-03-31')");
        $this->mysqli->query("INSERT INTO mi_dc_campaign VALUES (3, 'Youth Sports Equipment', 8000.00, 0.00, 'active', '2025-07-01', '2025-12-31')");

        // Donations
        $this->mysqli->query("INSERT INTO mi_dc_donation VALUES (1, 1, 1, 2000.00, '2025-01-15', 'Monthly pledge')");
        $this->mysqli->query("INSERT INTO mi_dc_donation VALUES (2, 1, 2, 1500.00, '2025-02-10', NULL)");
        $this->mysqli->query("INSERT INTO mi_dc_donation VALUES (3, 1, 1, 1000.00, '2025-03-01', 'Second donation')");
        $this->mysqli->query("INSERT INTO mi_dc_donation VALUES (4, 1, 3, 2000.00, '2025-04-15', 'Matching gift')");
        $this->mysqli->query("INSERT INTO mi_dc_donation VALUES (5, 2, 2, 3000.00, '2025-01-20', 'Corporate match')");
        $this->mysqli->query("INSERT INTO mi_dc_donation VALUES (6, 2, 4, 2200.00, '2025-02-28', NULL)");
    }

    /**
     * INSERT with column list in non-DDL order.
     * Verifies shadow store handles reordered columns correctly.
     */
    public function testInsertWithReorderedColumns(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_dc_donation (amount, donor_id, campaign_id, donated_at, id, note)
             VALUES (500.00, 1, 3, '2025-07-15', 7, 'First donation to new campaign')"
        );

        $rows = $this->ztdQuery(
            "SELECT id, campaign_id, donor_id, amount, note
             FROM mi_dc_donation
             WHERE id = 7"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(7, (int) $rows[0]['id']);
        $this->assertEquals(3, (int) $rows[0]['campaign_id']);
        $this->assertEquals(1, (int) $rows[0]['donor_id']);
        $this->assertEqualsWithDelta(500.00, (float) $rows[0]['amount'], 0.01);
        $this->assertSame('First donation to new campaign', $rows[0]['note']);
    }

    /**
     * Self-referencing UPDATE: increment campaign raised amount.
     * Campaign 3 starts at 0.00, add 500.00 => 500.00.
     */
    public function testSelfRefUpdateRaisedAmount(): void
    {
        $this->ztdExec("UPDATE mi_dc_campaign SET raised = raised + 500.00 WHERE id = 3");

        $rows = $this->ztdQuery("SELECT raised FROM mi_dc_campaign WHERE id = 3");
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(500.00, (float) $rows[0]['raised'], 0.01);
    }

    /**
     * Chained self-referencing UPDATEs: multiple donations incrementing raised.
     * Campaign 1 starts at 6500.00, +750 +250 => 7500.00.
     */
    public function testChainedSelfRefUpdates(): void
    {
        $this->ztdExec("UPDATE mi_dc_campaign SET raised = raised + 750.00 WHERE id = 1");
        $this->ztdExec("UPDATE mi_dc_campaign SET raised = raised + 250.00 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT raised FROM mi_dc_campaign WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(7500.00, (float) $rows[0]['raised'], 0.01);
    }

    /**
     * COUNT(DISTINCT donor_id) per campaign.
     * Campaign 1: 3 unique donors, Campaign 2: 2 unique donors, Campaign 3: 0.
     */
    public function testUniqueDonorsPerCampaign(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.title,
                    COUNT(DISTINCT d.donor_id) AS unique_donors
             FROM mi_dc_campaign c
             LEFT JOIN mi_dc_donation d ON d.campaign_id = c.id
             GROUP BY c.id, c.title
             ORDER BY c.id"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('School Library Fund', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['unique_donors']);

        $this->assertSame('Community Garden', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['unique_donors']);

        $this->assertSame('Youth Sports Equipment', $rows[2]['title']);
        $this->assertEquals(0, (int) $rows[2]['unique_donors']);
    }

    /**
     * COALESCE(SUM, 0) with LEFT JOIN: total donated per campaign including zero.
     * Campaign 1: 6500.00, Campaign 2: 5200.00, Campaign 3: 0.00.
     */
    public function testTotalDonatedWithZeroCampaign(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.title,
                    COALESCE(SUM(d.amount), 0) AS total_donated,
                    c.goal,
                    ROUND(COALESCE(SUM(d.amount), 0) * 100.0 / c.goal, 1) AS pct_funded
             FROM mi_dc_campaign c
             LEFT JOIN mi_dc_donation d ON d.campaign_id = c.id
             GROUP BY c.id, c.title, c.goal
             ORDER BY c.id"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('School Library Fund', $rows[0]['title']);
        $this->assertEqualsWithDelta(6500.00, (float) $rows[0]['total_donated'], 0.01);
        $this->assertEqualsWithDelta(65.0, (float) $rows[0]['pct_funded'], 0.1);

        $this->assertSame('Community Garden', $rows[1]['title']);
        $this->assertEqualsWithDelta(5200.00, (float) $rows[1]['total_donated'], 0.01);
        $this->assertEqualsWithDelta(104.0, (float) $rows[1]['pct_funded'], 0.1);

        $this->assertSame('Youth Sports Equipment', $rows[2]['title']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[2]['total_donated'], 0.01);
        $this->assertEqualsWithDelta(0.0, (float) $rows[2]['pct_funded'], 0.1);
    }

    /**
     * Donor giving summary: total donated across all campaigns per donor.
     * Alice: 3000.00, Bob: 4500.00, Carol: 2000.00, Dave: 2200.00.
     */
    public function testDonorGivingSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT dn.name,
                    COUNT(d.id) AS donation_count,
                    SUM(d.amount) AS total_given
             FROM mi_dc_donor dn
             JOIN mi_dc_donation d ON d.donor_id = dn.id
             GROUP BY dn.id, dn.name
             ORDER BY dn.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['donation_count']);
        $this->assertEqualsWithDelta(3000.00, (float) $rows[0]['total_given'], 0.01);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['donation_count']);
        $this->assertEqualsWithDelta(4500.00, (float) $rows[1]['total_given'], 0.01);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['donation_count']);
        $this->assertEqualsWithDelta(2000.00, (float) $rows[2]['total_given'], 0.01);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertEquals(1, (int) $rows[3]['donation_count']);
        $this->assertEqualsWithDelta(2200.00, (float) $rows[3]['total_given'], 0.01);
    }

    /**
     * DELETE donation + verify count.
     * Remove donation 6, then campaign 2 should have 1 donation totaling 3000.00.
     */
    public function testDeleteDonationAndVerify(): void
    {
        $affected = $this->ztdExec("DELETE FROM mi_dc_donation WHERE id = 6");
        $this->assertEquals(1, $affected);

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0) AS total
             FROM mi_dc_donation
             WHERE campaign_id = 2"
        );

        $this->assertEquals(1, (int) $rows[0]['cnt']);
        $this->assertEqualsWithDelta(3000.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Prepared statement: lookup donations by donor email.
     * alice@example.com => 2 donations to School Library Fund.
     */
    public function testPreparedDonationsByDonorEmail(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.amount, d.donated_at, c.title
             FROM mi_dc_donation d
             JOIN mi_dc_donor dn ON dn.id = d.donor_id
             JOIN mi_dc_campaign c ON c.id = d.campaign_id
             WHERE dn.email = ?
             ORDER BY d.donated_at",
            ['alice@example.com']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('School Library Fund', $rows[0]['title']);
        $this->assertEqualsWithDelta(2000.00, (float) $rows[0]['amount'], 0.01);
        $this->assertSame('School Library Fund', $rows[1]['title']);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['amount'], 0.01);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_dc_donation VALUES (7, 3, 1, 100.00, '2025-07-01', 'Test')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dc_donation");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dc_donation');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
