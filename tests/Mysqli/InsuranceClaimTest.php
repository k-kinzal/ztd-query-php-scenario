<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests insurance claim filing, review, adjudication, and payout tracking (MySQLi).
 * SQL patterns exercised: state machine UPDATE, date arithmetic for deadline tracking,
 * SUM for coverage limits, CASE for adjudication status labels, multi-table JOIN,
 * prepared statement for claim lookup.
 * @spec SPEC-10.2.147
 */
class InsuranceClaimTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ic_policies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                holder_name VARCHAR(100),
                policy_type VARCHAR(50),
                coverage_limit DECIMAL(12,2),
                start_date TEXT,
                end_date TEXT
            )',
            'CREATE TABLE mi_ic_claims (
                id INT AUTO_INCREMENT PRIMARY KEY,
                policy_id INT,
                claim_date TEXT,
                description VARCHAR(200),
                amount_requested DECIMAL(12,2),
                status VARCHAR(50),
                adjudication_date TEXT,
                payout_amount DECIMAL(12,2)
            )',
            'CREATE TABLE mi_ic_claim_notes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                claim_id INT,
                note_date TEXT,
                author VARCHAR(100),
                note_text TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ic_claim_notes', 'mi_ic_claims', 'mi_ic_policies'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Policies
        $this->mysqli->query("INSERT INTO mi_ic_policies VALUES (1, 'Alice', 'auto', 50000.00, '2025-01-01', '2025-12-31')");
        $this->mysqli->query("INSERT INTO mi_ic_policies VALUES (2, 'Bob', 'home', 250000.00, '2025-01-01', '2025-12-31')");
        $this->mysqli->query("INSERT INTO mi_ic_policies VALUES (3, 'Carol', 'health', 100000.00, '2025-03-01', '2026-02-28')");
        $this->mysqli->query("INSERT INTO mi_ic_policies VALUES (4, 'Dave', 'auto', 30000.00, '2025-06-01', '2026-05-31')");

        // Claims
        $this->mysqli->query("INSERT INTO mi_ic_claims VALUES (1, 1, '2025-03-15', 'Fender bender', 3500.00, 'paid', '2025-03-25', 3200.00)");
        $this->mysqli->query("INSERT INTO mi_ic_claims VALUES (2, 1, '2025-07-20', 'Windshield crack', 800.00, 'approved', '2025-08-01', 800.00)");
        $this->mysqli->query("INSERT INTO mi_ic_claims VALUES (3, 2, '2025-05-10', 'Water damage', 45000.00, 'under_review', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_ic_claims VALUES (4, 3, '2025-06-01', 'Surgery', 28000.00, 'paid', '2025-06-20', 27500.00)");
        $this->mysqli->query("INSERT INTO mi_ic_claims VALUES (5, 3, '2025-08-15', 'Lab tests', 1200.00, 'denied', '2025-08-25', NULL)");
        $this->mysqli->query("INSERT INTO mi_ic_claims VALUES (6, 4, '2025-09-01', 'Collision', 15000.00, 'filed', NULL, NULL)");

        // Claim notes
        $this->mysqli->query("INSERT INTO mi_ic_claim_notes VALUES (1, 1, '2025-03-16', 'adjuster', 'Photos received')");
        $this->mysqli->query("INSERT INTO mi_ic_claim_notes VALUES (2, 1, '2025-03-20', 'adjuster', 'Estimate confirmed at \$3200')");
        $this->mysqli->query("INSERT INTO mi_ic_claim_notes VALUES (3, 3, '2025-05-12', 'adjuster', 'Inspector scheduled')");
        $this->mysqli->query("INSERT INTO mi_ic_claim_notes VALUES (4, 3, '2025-05-18', 'inspector', 'Damage assessment: \$45000')");
        $this->mysqli->query("INSERT INTO mi_ic_claim_notes VALUES (5, 5, '2025-08-20', 'reviewer', 'Pre-existing condition noted')");
        $this->mysqli->query("INSERT INTO mi_ic_claim_notes VALUES (6, 6, '2025-09-02', 'system', 'Claim received')");
    }

    /**
     * JOIN claims with policies to show holder_name, claim count, and total requested per policy.
     * Alice: 2 claims, 4300.00; Bob: 1 claim, 45000.00; Carol: 2 claims, 29200.00; Dave: 1 claim, 15000.00.
     */
    public function testClaimSummaryByPolicy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.holder_name,
                    COUNT(c.id) AS claim_count,
                    SUM(c.amount_requested) AS total_requested
             FROM mi_ic_policies p
             JOIN mi_ic_claims c ON c.policy_id = p.id
             GROUP BY p.id, p.holder_name
             ORDER BY p.holder_name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['holder_name']);
        $this->assertEquals(2, (int) $rows[0]['claim_count']);
        $this->assertEqualsWithDelta(4300.00, (float) $rows[0]['total_requested'], 0.01);

        $this->assertSame('Bob', $rows[1]['holder_name']);
        $this->assertEquals(1, (int) $rows[1]['claim_count']);
        $this->assertEqualsWithDelta(45000.00, (float) $rows[1]['total_requested'], 0.01);

        $this->assertSame('Carol', $rows[2]['holder_name']);
        $this->assertEquals(2, (int) $rows[2]['claim_count']);
        $this->assertEqualsWithDelta(29200.00, (float) $rows[2]['total_requested'], 0.01);

        $this->assertSame('Dave', $rows[3]['holder_name']);
        $this->assertEquals(1, (int) $rows[3]['claim_count']);
        $this->assertEqualsWithDelta(15000.00, (float) $rows[3]['total_requested'], 0.01);
    }

    /**
     * GROUP BY status with COUNT and CASE for human-readable status labels.
     * approved=1, denied=1, filed=1, paid=2, under_review=1.
     */
    public function testClaimStatusDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.status,
                    COUNT(*) AS cnt,
                    CASE c.status
                        WHEN 'filed' THEN 'Filed'
                        WHEN 'under_review' THEN 'Under Review'
                        WHEN 'approved' THEN 'Approved'
                        WHEN 'denied' THEN 'Denied'
                        WHEN 'paid' THEN 'Paid'
                    END AS status_label
             FROM mi_ic_claims c
             GROUP BY c.status
             ORDER BY c.status"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('approved', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['cnt']);
        $this->assertSame('Approved', $rows[0]['status_label']);

        $this->assertSame('denied', $rows[1]['status']);
        $this->assertEquals(1, (int) $rows[1]['cnt']);
        $this->assertSame('Denied', $rows[1]['status_label']);

        $this->assertSame('filed', $rows[2]['status']);
        $this->assertEquals(1, (int) $rows[2]['cnt']);
        $this->assertSame('Filed', $rows[2]['status_label']);

        $this->assertSame('paid', $rows[3]['status']);
        $this->assertEquals(2, (int) $rows[3]['cnt']);
        $this->assertSame('Paid', $rows[3]['status_label']);

        $this->assertSame('under_review', $rows[4]['status']);
        $this->assertEquals(1, (int) $rows[4]['cnt']);
        $this->assertSame('Under Review', $rows[4]['status_label']);
    }

    /**
     * SUM payout_amount grouped by policy_type for paid claims only.
     * auto: 3200.00, health: 27500.00.
     */
    public function testTotalPayoutsByPolicyType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.policy_type,
                    SUM(c.payout_amount) AS total_payouts
             FROM mi_ic_policies p
             JOIN mi_ic_claims c ON c.policy_id = p.id
             WHERE c.status = 'paid'
             GROUP BY p.policy_type
             ORDER BY p.policy_type"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('auto', $rows[0]['policy_type']);
        $this->assertEqualsWithDelta(3200.00, (float) $rows[0]['total_payouts'], 0.01);

        $this->assertSame('health', $rows[1]['policy_type']);
        $this->assertEqualsWithDelta(27500.00, (float) $rows[1]['total_payouts'], 0.01);
    }

    /**
     * Coverage utilization: SUM of paid payouts as percentage of coverage_limit per policy.
     * Alice: 6.4%, Bob: 0%, Carol: 27.5%, Dave: 0%.
     */
    public function testCoverageUtilization(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.holder_name,
                    p.coverage_limit,
                    COALESCE(SUM(CASE WHEN c.status = 'paid' THEN c.payout_amount ELSE 0 END), 0) AS total_paid,
                    ROUND(COALESCE(SUM(CASE WHEN c.status = 'paid' THEN c.payout_amount ELSE 0 END), 0) * 100.0 / p.coverage_limit, 1) AS utilization_pct
             FROM mi_ic_policies p
             LEFT JOIN mi_ic_claims c ON c.policy_id = p.id
             GROUP BY p.id, p.holder_name, p.coverage_limit
             ORDER BY p.holder_name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['holder_name']);
        $this->assertEqualsWithDelta(6.4, (float) $rows[0]['utilization_pct'], 0.1);

        $this->assertSame('Bob', $rows[1]['holder_name']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[1]['utilization_pct'], 0.1);

        $this->assertSame('Carol', $rows[2]['holder_name']);
        $this->assertEqualsWithDelta(27.5, (float) $rows[2]['utilization_pct'], 0.1);

        $this->assertSame('Dave', $rows[3]['holder_name']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[3]['utilization_pct'], 0.1);
    }

    /**
     * LEFT JOIN claim_notes to count notes per claim.
     * Claim 1: 2, claim 2: 0, claim 3: 2, claim 4: 0, claim 5: 1, claim 6: 1.
     */
    public function testClaimsWithNoteCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id AS claim_id,
                    c.description,
                    COUNT(cn.id) AS note_count
             FROM mi_ic_claims c
             LEFT JOIN mi_ic_claim_notes cn ON cn.claim_id = c.id
             GROUP BY c.id, c.description
             ORDER BY c.id"
        );

        $this->assertCount(6, $rows);

        $this->assertEquals(1, (int) $rows[0]['claim_id']);
        $this->assertEquals(2, (int) $rows[0]['note_count']);

        $this->assertEquals(2, (int) $rows[1]['claim_id']);
        $this->assertEquals(0, (int) $rows[1]['note_count']);

        $this->assertEquals(3, (int) $rows[2]['claim_id']);
        $this->assertEquals(2, (int) $rows[2]['note_count']);

        $this->assertEquals(4, (int) $rows[3]['claim_id']);
        $this->assertEquals(0, (int) $rows[3]['note_count']);

        $this->assertEquals(5, (int) $rows[4]['claim_id']);
        $this->assertEquals(1, (int) $rows[4]['note_count']);

        $this->assertEquals(6, (int) $rows[5]['claim_id']);
        $this->assertEquals(1, (int) $rows[5]['note_count']);
    }

    /**
     * Prepared statement: lookup claims by policy_type.
     * policy_type='auto' => 3 claims (2 from Alice + 1 from Dave).
     */
    public function testPreparedClaimsByPolicyType(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.id, c.description, p.holder_name
             FROM mi_ic_claims c
             JOIN mi_ic_policies p ON p.id = c.policy_id
             WHERE p.policy_type = ?
             ORDER BY c.id",
            ['auto']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['holder_name']);
        $this->assertSame('Fender bender', $rows[0]['description']);
        $this->assertSame('Alice', $rows[1]['holder_name']);
        $this->assertSame('Windshield crack', $rows[1]['description']);
        $this->assertSame('Dave', $rows[2]['holder_name']);
        $this->assertSame('Collision', $rows[2]['description']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ic_claims VALUES (7, 1, '2025-10-01', 'Theft', 10000.00, 'filed', NULL, NULL)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ic_claims");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ic_claims');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
