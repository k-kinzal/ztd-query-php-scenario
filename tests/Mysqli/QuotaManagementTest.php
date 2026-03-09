<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a quota management scenario: cumulative SUM with running totals,
 * CASE-based threshold evaluation, prepared statement with multiple parameters,
 * and percentage calculations (MySQLi).
 * @spec SPEC-10.2.131
 */
class QuotaManagementTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_qm_plans (
                id INT AUTO_INCREMENT PRIMARY KEY,
                plan_name VARCHAR(50),
                storage_limit_gb INT,
                api_calls_limit INT,
                users_limit INT
            )',
            'CREATE TABLE mi_qm_accounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_name VARCHAR(100),
                plan_id INT,
                created_at VARCHAR(20)
            )',
            'CREATE TABLE mi_qm_usage_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_id INT,
                usage_date VARCHAR(20),
                storage_used_gb DECIMAL(10,2),
                api_calls INT,
                active_users INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_qm_usage_log', 'mi_qm_accounts', 'mi_qm_plans'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Plans
        $this->mysqli->query("INSERT INTO mi_qm_plans VALUES (1, 'starter', 10, 1000, 5)");
        $this->mysqli->query("INSERT INTO mi_qm_plans VALUES (2, 'pro', 100, 10000, 25)");
        $this->mysqli->query("INSERT INTO mi_qm_plans VALUES (3, 'enterprise', 1000, 100000, 500)");

        // Accounts
        $this->mysqli->query("INSERT INTO mi_qm_accounts VALUES (1, 'Acme Corp', 2, '2025-01-01')");
        $this->mysqli->query("INSERT INTO mi_qm_accounts VALUES (2, 'Widgets Inc', 1, '2025-03-15')");
        $this->mysqli->query("INSERT INTO mi_qm_accounts VALUES (3, 'Big Bank', 3, '2025-02-01')");
        $this->mysqli->query("INSERT INTO mi_qm_accounts VALUES (4, 'Startup X', 1, '2025-06-01')");

        // Usage Log — Acme Corp (pro plan)
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (1, 1, '2025-10-01', 45.50, 3200, 18)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (2, 1, '2025-10-02', 46.00, 2800, 18)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (3, 1, '2025-10-03', 48.20, 4100, 20)");

        // Usage Log — Widgets Inc (starter plan — near limit)
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (4, 2, '2025-10-01', 8.50, 800, 4)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (5, 2, '2025-10-02', 9.10, 950, 5)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (6, 2, '2025-10-03', 9.80, 1100, 5)");

        // Usage Log — Big Bank (enterprise plan)
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (7, 3, '2025-10-01', 250.00, 45000, 180)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (8, 3, '2025-10-02', 252.00, 48000, 185)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (9, 3, '2025-10-03', 255.00, 52000, 190)");

        // Usage Log — Startup X (starter plan — low usage)
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (10, 4, '2025-10-01', 1.20, 100, 2)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (11, 4, '2025-10-02', 1.50, 150, 2)");
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (12, 4, '2025-10-03', 1.80, 120, 3)");
    }

    /**
     * Get the most recent usage snapshot per account using a correlated subquery.
     */
    public function testLatestUsagePerAccount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.account_name, p.plan_name,
                    u.storage_used_gb, u.api_calls, u.active_users
             FROM mi_qm_accounts a
             JOIN mi_qm_plans p ON p.id = a.plan_id
             JOIN mi_qm_usage_log u ON u.account_id = a.id
             WHERE u.usage_date = (
                 SELECT MAX(u2.usage_date)
                 FROM mi_qm_usage_log u2
                 WHERE u2.account_id = a.id
             )
             ORDER BY a.account_name"
        );

        $this->assertCount(4, $rows);

        // Acme Corp: pro, 48.20, 4100, 20
        $this->assertSame('Acme Corp', $rows[0]['account_name']);
        $this->assertSame('pro', $rows[0]['plan_name']);
        $this->assertEqualsWithDelta(48.20, (float) $rows[0]['storage_used_gb'], 0.01);
        $this->assertEquals(4100, (int) $rows[0]['api_calls']);
        $this->assertEquals(20, (int) $rows[0]['active_users']);

        // Big Bank: enterprise, 255.00, 52000, 190
        $this->assertSame('Big Bank', $rows[1]['account_name']);
        $this->assertSame('enterprise', $rows[1]['plan_name']);
        $this->assertEqualsWithDelta(255.00, (float) $rows[1]['storage_used_gb'], 0.01);
        $this->assertEquals(52000, (int) $rows[1]['api_calls']);
        $this->assertEquals(190, (int) $rows[1]['active_users']);

        // Startup X: starter, 1.80, 120, 3
        $this->assertSame('Startup X', $rows[2]['account_name']);
        $this->assertSame('starter', $rows[2]['plan_name']);
        $this->assertEqualsWithDelta(1.80, (float) $rows[2]['storage_used_gb'], 0.01);
        $this->assertEquals(120, (int) $rows[2]['api_calls']);
        $this->assertEquals(3, (int) $rows[2]['active_users']);

        // Widgets Inc: starter, 9.80, 1100, 5
        $this->assertSame('Widgets Inc', $rows[3]['account_name']);
        $this->assertSame('starter', $rows[3]['plan_name']);
        $this->assertEqualsWithDelta(9.80, (float) $rows[3]['storage_used_gb'], 0.01);
        $this->assertEquals(1100, (int) $rows[3]['api_calls']);
        $this->assertEquals(5, (int) $rows[3]['active_users']);
    }

    /**
     * Calculate percentage utilization for each resource on the latest day.
     */
    public function testQuotaUtilization(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.account_name, p.plan_name,
                    u.storage_used_gb, p.storage_limit_gb,
                    ROUND(u.storage_used_gb * 100.0 / p.storage_limit_gb, 1) AS storage_pct,
                    u.api_calls, p.api_calls_limit,
                    ROUND(u.api_calls * 100.0 / p.api_calls_limit, 1) AS api_pct,
                    u.active_users, p.users_limit,
                    ROUND(u.active_users * 100.0 / p.users_limit, 1) AS users_pct
             FROM mi_qm_accounts a
             JOIN mi_qm_plans p ON p.id = a.plan_id
             JOIN mi_qm_usage_log u ON u.account_id = a.id
             WHERE u.usage_date = '2025-10-03'
             ORDER BY a.account_name"
        );

        $this->assertCount(4, $rows);

        // Acme Corp: storage=48.2%, api=41.0%, users=80.0%
        $this->assertSame('Acme Corp', $rows[0]['account_name']);
        $this->assertEqualsWithDelta(48.2, (float) $rows[0]['storage_pct'], 0.01);
        $this->assertEqualsWithDelta(41.0, (float) $rows[0]['api_pct'], 0.01);
        $this->assertEqualsWithDelta(80.0, (float) $rows[0]['users_pct'], 0.01);

        // Big Bank: storage=25.5%, api=52.0%, users=38.0%
        $this->assertSame('Big Bank', $rows[1]['account_name']);
        $this->assertEqualsWithDelta(25.5, (float) $rows[1]['storage_pct'], 0.01);
        $this->assertEqualsWithDelta(52.0, (float) $rows[1]['api_pct'], 0.01);
        $this->assertEqualsWithDelta(38.0, (float) $rows[1]['users_pct'], 0.01);

        // Startup X: storage=18.0%, api=12.0%, users=60.0%
        $this->assertSame('Startup X', $rows[2]['account_name']);
        $this->assertEqualsWithDelta(18.0, (float) $rows[2]['storage_pct'], 0.01);
        $this->assertEqualsWithDelta(12.0, (float) $rows[2]['api_pct'], 0.01);
        $this->assertEqualsWithDelta(60.0, (float) $rows[2]['users_pct'], 0.01);

        // Widgets Inc: storage=98.0%, api=110.0%, users=100.0%
        $this->assertSame('Widgets Inc', $rows[3]['account_name']);
        $this->assertEqualsWithDelta(98.0, (float) $rows[3]['storage_pct'], 0.01);
        $this->assertEqualsWithDelta(110.0, (float) $rows[3]['api_pct'], 0.01);
        $this->assertEqualsWithDelta(100.0, (float) $rows[3]['users_pct'], 0.01);
    }

    /**
     * Find accounts that exceed ANY quota on any day using CASE to flag.
     */
    public function testOverQuotaAccounts(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.account_name, p.plan_name, u.usage_date,
                    CASE
                        WHEN u.storage_used_gb > p.storage_limit_gb THEN 'storage'
                        WHEN u.api_calls > p.api_calls_limit THEN 'api_calls'
                        WHEN u.active_users > p.users_limit THEN 'users'
                    END AS exceeded_resource
             FROM mi_qm_accounts a
             JOIN mi_qm_plans p ON p.id = a.plan_id
             JOIN mi_qm_usage_log u ON u.account_id = a.id
             WHERE u.storage_used_gb > p.storage_limit_gb
                OR u.api_calls > p.api_calls_limit
                OR u.active_users > p.users_limit
             ORDER BY a.account_name, u.usage_date"
        );

        // Only Widgets Inc on 2025-10-03 exceeds api_calls (1100 > 1000)
        $this->assertCount(1, $rows);
        $this->assertSame('Widgets Inc', $rows[0]['account_name']);
        $this->assertSame('starter', $rows[0]['plan_name']);
        $this->assertSame('2025-10-03', $rows[0]['usage_date']);
        $this->assertSame('api_calls', $rows[0]['exceeded_resource']);
    }

    /**
     * Calculate daily totals across all accounts.
     */
    public function testUsageTrend(): void
    {
        $rows = $this->ztdQuery(
            "SELECT usage_date,
                    SUM(storage_used_gb) AS total_storage,
                    SUM(api_calls) AS total_api,
                    SUM(active_users) AS total_users
             FROM mi_qm_usage_log
             GROUP BY usage_date
             ORDER BY usage_date"
        );

        $this->assertCount(3, $rows);

        // 2025-10-01: 305.20, 49100, 204
        $this->assertSame('2025-10-01', $rows[0]['usage_date']);
        $this->assertEqualsWithDelta(305.20, (float) $rows[0]['total_storage'], 0.01);
        $this->assertEquals(49100, (int) $rows[0]['total_api']);
        $this->assertEquals(204, (int) $rows[0]['total_users']);

        // 2025-10-02: 308.60, 51900, 210
        $this->assertSame('2025-10-02', $rows[1]['usage_date']);
        $this->assertEqualsWithDelta(308.60, (float) $rows[1]['total_storage'], 0.01);
        $this->assertEquals(51900, (int) $rows[1]['total_api']);
        $this->assertEquals(210, (int) $rows[1]['total_users']);

        // 2025-10-03: 314.80, 57320, 218
        $this->assertSame('2025-10-03', $rows[2]['usage_date']);
        $this->assertEqualsWithDelta(314.80, (float) $rows[2]['total_storage'], 0.01);
        $this->assertEquals(57320, (int) $rows[2]['total_api']);
        $this->assertEquals(218, (int) $rows[2]['total_users']);
    }

    /**
     * Accounts using >80% of ANY resource on latest day.
     */
    public function testHighRiskAccounts(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.account_name, p.plan_name,
                    ROUND(u.storage_used_gb * 100.0 / p.storage_limit_gb, 1) AS storage_pct,
                    ROUND(u.api_calls * 100.0 / p.api_calls_limit, 1) AS api_pct,
                    ROUND(u.active_users * 100.0 / p.users_limit, 1) AS users_pct
             FROM mi_qm_accounts a
             JOIN mi_qm_plans p ON p.id = a.plan_id
             JOIN mi_qm_usage_log u ON u.account_id = a.id
             WHERE u.usage_date = '2025-10-03'
               AND (u.storage_used_gb * 100.0 / p.storage_limit_gb > 80
                    OR u.api_calls * 100.0 / p.api_calls_limit > 80
                    OR u.active_users * 100.0 / p.users_limit > 80)
             ORDER BY a.account_name"
        );

        // Acme Corp users=80.0% is exactly 80, NOT > 80 — excluded
        // Only Widgets Inc qualifies (storage=98%, api=110%, users=100%)
        $this->assertCount(1, $rows);
        $this->assertSame('Widgets Inc', $rows[0]['account_name']);
        $this->assertSame('starter', $rows[0]['plan_name']);
        $this->assertEqualsWithDelta(98.0, (float) $rows[0]['storage_pct'], 0.01);
        $this->assertEqualsWithDelta(110.0, (float) $rows[0]['api_pct'], 0.01);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['users_pct'], 0.01);
    }

    /**
     * Average daily usage per account across all recorded days.
     */
    public function testAvgDailyUsagePerAccount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.account_name,
                    ROUND(AVG(u.storage_used_gb), 2) AS avg_storage,
                    ROUND(AVG(u.api_calls), 0) AS avg_api,
                    ROUND(AVG(u.active_users), 0) AS avg_users
             FROM mi_qm_accounts a
             JOIN mi_qm_usage_log u ON u.account_id = a.id
             GROUP BY a.id, a.account_name
             ORDER BY a.account_name"
        );

        $this->assertCount(4, $rows);

        // Acme Corp: (45.5+46.0+48.2)/3=46.57, (3200+2800+4100)/3=3367, (18+18+20)/3=19
        $this->assertSame('Acme Corp', $rows[0]['account_name']);
        $this->assertEqualsWithDelta(46.57, (float) $rows[0]['avg_storage'], 0.01);
        $this->assertEqualsWithDelta(3367, (int) $rows[0]['avg_api'], 1);
        $this->assertEqualsWithDelta(19, (int) $rows[0]['avg_users'], 1);

        // Big Bank: (250+252+255)/3=252.33, (45000+48000+52000)/3=48333, (180+185+190)/3=185
        $this->assertSame('Big Bank', $rows[1]['account_name']);
        $this->assertEqualsWithDelta(252.33, (float) $rows[1]['avg_storage'], 0.01);
        $this->assertEqualsWithDelta(48333, (int) $rows[1]['avg_api'], 1);
        $this->assertEqualsWithDelta(185, (int) $rows[1]['avg_users'], 1);

        // Startup X: (1.2+1.5+1.8)/3=1.50, (100+150+120)/3=123, (2+2+3)/3=2
        $this->assertSame('Startup X', $rows[2]['account_name']);
        $this->assertEqualsWithDelta(1.50, (float) $rows[2]['avg_storage'], 0.01);
        $this->assertEqualsWithDelta(123, (int) $rows[2]['avg_api'], 1);
        $this->assertEqualsWithDelta(2, (int) $rows[2]['avg_users'], 1);

        // Widgets Inc: (8.5+9.1+9.8)/3=9.13, (800+950+1100)/3=950, (4+5+5)/3=5
        $this->assertSame('Widgets Inc', $rows[3]['account_name']);
        $this->assertEqualsWithDelta(9.13, (float) $rows[3]['avg_storage'], 0.01);
        $this->assertEqualsWithDelta(950, (int) $rows[3]['avg_api'], 1);
        $this->assertEqualsWithDelta(5, (int) $rows[3]['avg_users'], 1);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_qm_usage_log VALUES (13, 1, '2025-10-04', 50.00, 5000, 22)");
        $this->mysqli->query("UPDATE mi_qm_accounts SET account_name = 'Acme Holdings' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_qm_usage_log");
        $this->assertEquals(13, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT account_name FROM mi_qm_accounts WHERE id = 1");
        $this->assertSame('Acme Holdings', $rows[0]['account_name']);

        // Physical tables untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_qm_usage_log');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
