<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a tenant usage metering scenario through ZTD shadow store (MySQLi).
 * SaaS API usage tracking with quota enforcement, monthly aggregation,
 * and overage detection.
 * SQL patterns exercised: SUM for request aggregation, GROUP BY tenant+month,
 * SUBSTR for date extraction, ROUND with CAST for percentage calculation,
 * CASE for quota status, HAVING for threshold filtering, LEFT JOIN with COALESCE
 * for overage charges, prepared statement for tenant lookup, physical isolation.
 * @spec SPEC-10.2.156
 */
class UsageMeteringTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_um_tenants (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                plan_name VARCHAR(100),
                monthly_quota INT
            )',
            'CREATE TABLE mi_um_usage_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tenant_id INT,
                endpoint VARCHAR(100),
                recorded_at TEXT,
                request_count INT
            )',
            'CREATE TABLE mi_um_overage_charges (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tenant_id INT,
                month VARCHAR(10),
                overage_units INT,
                charge_amount DECIMAL(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_um_overage_charges', 'mi_um_usage_records', 'mi_um_tenants'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Tenants
        $this->mysqli->query("INSERT INTO mi_um_tenants VALUES (1, 'Acme Corp', 'pro', 10000)");
        $this->mysqli->query("INSERT INTO mi_um_tenants VALUES (2, 'Beta Inc', 'starter', 1000)");
        $this->mysqli->query("INSERT INTO mi_um_tenants VALUES (3, 'Gamma LLC', 'pro', 10000)");
        $this->mysqli->query("INSERT INTO mi_um_tenants VALUES (4, 'Delta Co', 'enterprise', 50000)");

        // Usage records
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (1, 1, '/api/users', '2026-01-15', 1500)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (2, 1, '/api/users', '2026-01-20', 1200)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (3, 1, '/api/orders', '2026-01-18', 800)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (4, 1, '/api/orders', '2026-01-25', 900)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (5, 2, '/api/users', '2026-01-10', 600)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (6, 2, '/api/users', '2026-01-22', 500)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (7, 3, '/api/users', '2026-01-12', 2000)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (8, 3, '/api/users', '2026-01-28', 1800)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (9, 3, '/api/orders', '2026-01-16', 1500)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (10, 3, '/api/orders', '2026-01-30', 1200)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (11, 4, '/api/users', '2026-01-05', 5000)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (12, 4, '/api/users', '2026-01-19', 4500)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (13, 4, '/api/orders', '2026-01-10', 3000)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (14, 4, '/api/orders', '2026-01-26', 2500)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (15, 1, '/api/users', '2026-02-05', 1600)");
        $this->mysqli->query("INSERT INTO mi_um_usage_records VALUES (16, 2, '/api/users', '2026-02-12', 700)");

        // Overage charges
        $this->mysqli->query("INSERT INTO mi_um_overage_charges VALUES (1, 2, '2026-01', 100, 10.00)");
    }

    /**
     * GROUP BY tenant name + SUBSTR(recorded_at, 1, 7) for month, SUM(request_count).
     * JOIN tenants to usage_records. ORDER BY tenant name, month.
     * Acme Corp 2026-01: 4400, Acme Corp 2026-02: 1600, Beta Inc 2026-01: 1100,
     * Beta Inc 2026-02: 700, Delta Co 2026-01: 15000, Gamma LLC 2026-01: 6500.
     */
    public function testMonthlyUsageSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name, SUBSTR(u.recorded_at, 1, 7) AS month,
                    SUM(u.request_count) AS total_requests
             FROM mi_um_tenants t
             JOIN mi_um_usage_records u ON u.tenant_id = t.id
             GROUP BY t.name, month
             ORDER BY t.name, month"
        );

        $this->assertCount(6, $rows);

        $this->assertSame('Acme Corp', $rows[0]['name']);
        $this->assertSame('2026-01', $rows[0]['month']);
        $this->assertEquals(4400, (int) $rows[0]['total_requests']);

        $this->assertSame('Acme Corp', $rows[1]['name']);
        $this->assertSame('2026-02', $rows[1]['month']);
        $this->assertEquals(1600, (int) $rows[1]['total_requests']);

        $this->assertSame('Beta Inc', $rows[2]['name']);
        $this->assertSame('2026-01', $rows[2]['month']);
        $this->assertEquals(1100, (int) $rows[2]['total_requests']);

        $this->assertSame('Beta Inc', $rows[3]['name']);
        $this->assertSame('2026-02', $rows[3]['month']);
        $this->assertEquals(700, (int) $rows[3]['total_requests']);

        $this->assertSame('Delta Co', $rows[4]['name']);
        $this->assertSame('2026-01', $rows[4]['month']);
        $this->assertEquals(15000, (int) $rows[4]['total_requests']);

        $this->assertSame('Gamma LLC', $rows[5]['name']);
        $this->assertSame('2026-01', $rows[5]['month']);
        $this->assertEquals(6500, (int) $rows[5]['total_requests']);
    }

    /**
     * GROUP BY tenant name + endpoint, SUM(request_count), COUNT(*) as record_count.
     * WHERE SUBSTR(recorded_at, 1, 7) = '2026-01'. ORDER BY tenant name, endpoint.
     */
    public function testEndpointBreakdown(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name, u.endpoint,
                    SUM(u.request_count) AS total_requests,
                    COUNT(*) AS record_count
             FROM mi_um_tenants t
             JOIN mi_um_usage_records u ON u.tenant_id = t.id
             WHERE SUBSTR(u.recorded_at, 1, 7) = '2026-01'
             GROUP BY t.name, u.endpoint
             ORDER BY t.name, u.endpoint"
        );

        $this->assertCount(7, $rows);

        $this->assertSame('Acme Corp', $rows[0]['name']);
        $this->assertSame('/api/orders', $rows[0]['endpoint']);
        $this->assertEquals(1700, (int) $rows[0]['total_requests']);
        $this->assertEquals(2, (int) $rows[0]['record_count']);

        $this->assertSame('Acme Corp', $rows[1]['name']);
        $this->assertSame('/api/users', $rows[1]['endpoint']);
        $this->assertEquals(2700, (int) $rows[1]['total_requests']);
        $this->assertEquals(2, (int) $rows[1]['record_count']);

        $this->assertSame('Beta Inc', $rows[2]['name']);
        $this->assertSame('/api/users', $rows[2]['endpoint']);
        $this->assertEquals(1100, (int) $rows[2]['total_requests']);
        $this->assertEquals(2, (int) $rows[2]['record_count']);

        $this->assertSame('Delta Co', $rows[3]['name']);
        $this->assertSame('/api/orders', $rows[3]['endpoint']);
        $this->assertEquals(5500, (int) $rows[3]['total_requests']);
        $this->assertEquals(2, (int) $rows[3]['record_count']);

        $this->assertSame('Delta Co', $rows[4]['name']);
        $this->assertSame('/api/users', $rows[4]['endpoint']);
        $this->assertEquals(9500, (int) $rows[4]['total_requests']);
        $this->assertEquals(2, (int) $rows[4]['record_count']);

        $this->assertSame('Gamma LLC', $rows[5]['name']);
        $this->assertSame('/api/orders', $rows[5]['endpoint']);
        $this->assertEquals(2700, (int) $rows[5]['total_requests']);
        $this->assertEquals(2, (int) $rows[5]['record_count']);

        $this->assertSame('Gamma LLC', $rows[6]['name']);
        $this->assertSame('/api/users', $rows[6]['endpoint']);
        $this->assertEquals(3800, (int) $rows[6]['total_requests']);
        $this->assertEquals(2, (int) $rows[6]['record_count']);
    }

    /**
     * For January: JOIN tenants to usage_records, GROUP BY tenant, compute:
     * total_used, quota, utilization_pct (ROUND with CAST to avoid integer division),
     * status via CASE. ORDER BY utilization_pct DESC.
     * Beta Inc: 1100/1000=110.0 over, Gamma LLC: 6500/10000=65.0 under,
     * Acme Corp: 4400/10000=44.0 under, Delta Co: 15000/50000=30.0 under.
     */
    public function testQuotaUtilization(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name,
                    SUM(u.request_count) AS total_used,
                    t.monthly_quota AS quota,
                    ROUND(CAST(SUM(u.request_count) AS REAL) * 100.0 / t.monthly_quota, 1) AS utilization_pct,
                    CASE WHEN SUM(u.request_count) > t.monthly_quota THEN 'over' ELSE 'under' END AS status
             FROM mi_um_tenants t
             JOIN mi_um_usage_records u ON u.tenant_id = t.id
             WHERE SUBSTR(u.recorded_at, 1, 7) = '2026-01'
             GROUP BY t.id, t.name, t.monthly_quota
             ORDER BY utilization_pct DESC"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Beta Inc', $rows[0]['name']);
        $this->assertEquals(1100, (int) $rows[0]['total_used']);
        $this->assertEquals(1000, (int) $rows[0]['quota']);
        $this->assertEqualsWithDelta(110.0, (float) $rows[0]['utilization_pct'], 0.1);
        $this->assertSame('over', $rows[0]['status']);

        $this->assertSame('Gamma LLC', $rows[1]['name']);
        $this->assertEquals(6500, (int) $rows[1]['total_used']);
        $this->assertEquals(10000, (int) $rows[1]['quota']);
        $this->assertEqualsWithDelta(65.0, (float) $rows[1]['utilization_pct'], 0.1);
        $this->assertSame('under', $rows[1]['status']);

        $this->assertSame('Acme Corp', $rows[2]['name']);
        $this->assertEquals(4400, (int) $rows[2]['total_used']);
        $this->assertEquals(10000, (int) $rows[2]['quota']);
        $this->assertEqualsWithDelta(44.0, (float) $rows[2]['utilization_pct'], 0.1);
        $this->assertSame('under', $rows[2]['status']);

        $this->assertSame('Delta Co', $rows[3]['name']);
        $this->assertEquals(15000, (int) $rows[3]['total_used']);
        $this->assertEquals(50000, (int) $rows[3]['quota']);
        $this->assertEqualsWithDelta(30.0, (float) $rows[3]['utilization_pct'], 0.1);
        $this->assertSame('under', $rows[3]['status']);
    }

    /**
     * HAVING SUM(request_count) > t.monthly_quota for January.
     * Only Beta Inc (1100 > 1000).
     */
    public function testOverQuotaTenants(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name,
                    SUM(u.request_count) AS total_used,
                    t.monthly_quota AS quota
             FROM mi_um_tenants t
             JOIN mi_um_usage_records u ON u.tenant_id = t.id
             WHERE SUBSTR(u.recorded_at, 1, 7) = '2026-01'
             GROUP BY t.id, t.name, t.monthly_quota
             HAVING SUM(u.request_count) > t.monthly_quota"
        );

        $this->assertCount(1, $rows);

        $this->assertSame('Beta Inc', $rows[0]['name']);
        $this->assertEquals(1100, (int) $rows[0]['total_used']);
        $this->assertEquals(1000, (int) $rows[0]['quota']);
    }

    /**
     * LEFT JOIN tenants to overage_charges, show all tenants with
     * COALESCE(charge_amount, 0) for January. ORDER BY tenant name.
     * Acme Corp: 0.00, Beta Inc: 10.00, Delta Co: 0.00, Gamma LLC: 0.00.
     */
    public function testOverageChargeJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name,
                    COALESCE(oc.charge_amount, 0) AS charge
             FROM mi_um_tenants t
             LEFT JOIN mi_um_overage_charges oc ON oc.tenant_id = t.id AND oc.month = '2026-01'
             ORDER BY t.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Acme Corp', $rows[0]['name']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[0]['charge'], 0.01);

        $this->assertSame('Beta Inc', $rows[1]['name']);
        $this->assertEqualsWithDelta(10.00, (float) $rows[1]['charge'], 0.01);

        $this->assertSame('Delta Co', $rows[2]['name']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[2]['charge'], 0.01);

        $this->assertSame('Gamma LLC', $rows[3]['name']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[3]['charge'], 0.01);
    }

    /**
     * Prepared statement: find usage for a specific tenant_id and date range.
     * WHERE tenant_id = ? AND recorded_at >= ? AND recorded_at <= ?
     * with params [1, '2026-01-01', '2026-01-31']. Expected 4 rows for Acme Corp January.
     * ORDER BY recorded_at.
     */
    public function testPreparedTenantUsage(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT u.endpoint, u.recorded_at, u.request_count
             FROM mi_um_usage_records u
             WHERE u.tenant_id = ? AND u.recorded_at >= ? AND u.recorded_at <= ?
             ORDER BY u.recorded_at",
            [1, '2026-01-01', '2026-01-31']
        );

        $this->assertCount(4, $rows);

        $this->assertSame('/api/users', $rows[0]['endpoint']);
        $this->assertSame('2026-01-15', $rows[0]['recorded_at']);
        $this->assertEquals(1500, (int) $rows[0]['request_count']);

        $this->assertSame('/api/orders', $rows[1]['endpoint']);
        $this->assertSame('2026-01-18', $rows[1]['recorded_at']);
        $this->assertEquals(800, (int) $rows[1]['request_count']);

        $this->assertSame('/api/users', $rows[2]['endpoint']);
        $this->assertSame('2026-01-20', $rows[2]['recorded_at']);
        $this->assertEquals(1200, (int) $rows[2]['request_count']);

        $this->assertSame('/api/orders', $rows[3]['endpoint']);
        $this->assertSame('2026-01-25', $rows[3]['recorded_at']);
        $this->assertEquals(900, (int) $rows[3]['request_count']);
    }

    /**
     * Physical isolation: insert a new usage_record through ZTD, verify shadow
     * count is 17, then disableZtd and verify physical count is 0.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new usage record through ZTD
        $this->mysqli->query(
            "INSERT INTO mi_um_usage_records VALUES (17, 1, '/api/users', '2026-02-10', 500)"
        );

        // ZTD sees the new record (16 original + 1 new = 17)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_um_usage_records");
        $this->assertEquals(17, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_um_usage_records');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
