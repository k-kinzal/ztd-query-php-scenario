<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests an API key lifecycle scenario with usage quota tracking:
 * key status summary, daily usage counts, quota utilization, error rates,
 * response time by tier, and prepared statement key lookup.
 * SQL patterns exercised: COUNT within time window, GROUP BY date,
 * CASE for tier limit labels, percentage calculations (ROUND),
 * prepared statement for key lookup, SUM aggregate for usage totals (MySQL PDO).
 * @spec SPEC-10.2.148
 */
class MysqlApiKeyManagementTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ak_api_clients (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_name VARCHAR(100),
                tier VARCHAR(20),
                daily_quota INT,
                created_date TEXT
            )',
            'CREATE TABLE mp_ak_api_keys (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT,
                key_prefix VARCHAR(50),
                status VARCHAR(20),
                created_date TEXT,
                expires_date TEXT
            )',
            'CREATE TABLE mp_ak_api_usage (
                id INT AUTO_INCREMENT PRIMARY KEY,
                key_id INT,
                request_date TEXT,
                endpoint VARCHAR(100),
                response_code INT,
                response_time_ms INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ak_api_usage', 'mp_ak_api_keys', 'mp_ak_api_clients'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Clients
        $this->pdo->exec("INSERT INTO mp_ak_api_clients VALUES (1, 'Acme Corp', 'enterprise', 10000, '2025-01-01')");
        $this->pdo->exec("INSERT INTO mp_ak_api_clients VALUES (2, 'StartupXYZ', 'pro', 1000, '2025-03-15')");
        $this->pdo->exec("INSERT INTO mp_ak_api_clients VALUES (3, 'HobbyDev', 'free', 100, '2025-06-01')");

        // API Keys
        $this->pdo->exec("INSERT INTO mp_ak_api_keys VALUES (1, 1, 'sk_live_acme1', 'active', '2025-01-01', '2026-01-01')");
        $this->pdo->exec("INSERT INTO mp_ak_api_keys VALUES (2, 1, 'sk_live_acme2', 'revoked', '2025-01-01', '2026-01-01')");
        $this->pdo->exec("INSERT INTO mp_ak_api_keys VALUES (3, 2, 'sk_live_start1', 'active', '2025-03-15', '2025-12-31')");
        $this->pdo->exec("INSERT INTO mp_ak_api_keys VALUES (4, 3, 'sk_live_hobby1', 'active', '2025-06-01', '2025-12-31')");
        $this->pdo->exec("INSERT INTO mp_ak_api_keys VALUES (5, 3, 'sk_live_hobby2', 'expired', '2025-06-01', '2025-09-01')");

        // Usage — Key 1 (Acme active), 2025-10-01
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (1, 1, '2025-10-01', '/api/v1/users', 200, 45)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (2, 1, '2025-10-01', '/api/v1/users', 200, 52)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (3, 1, '2025-10-01', '/api/v1/orders', 201, 120)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (4, 1, '2025-10-01', '/api/v1/orders', 200, 88)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (5, 1, '2025-10-01', '/api/v1/health', 200, 12)");

        // Usage — Key 1, 2025-10-02
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (6, 1, '2025-10-02', '/api/v1/users', 200, 38)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (7, 1, '2025-10-02', '/api/v1/orders', 500, 250)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (8, 1, '2025-10-02', '/api/v1/orders', 200, 95)");

        // Usage — Key 3 (Startup active), 2025-10-01
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (9, 3, '2025-10-01', '/api/v1/users', 200, 65)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (10, 3, '2025-10-01', '/api/v1/products', 200, 78)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (11, 3, '2025-10-01', '/api/v1/products', 200, 82)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (12, 3, '2025-10-01', '/api/v1/health', 200, 15)");

        // Usage — Key 3, 2025-10-02
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (13, 3, '2025-10-02', '/api/v1/users', 200, 55)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (14, 3, '2025-10-02', '/api/v1/products', 200, 70)");

        // Usage — Key 4 (Hobby active), 2025-10-01
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (15, 4, '2025-10-01', '/api/v1/users', 200, 110)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (16, 4, '2025-10-01', '/api/v1/users', 200, 105)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (17, 4, '2025-10-01', '/api/v1/health', 200, 20)");

        // Usage — Key 5 (Hobby expired), 2025-09-15 (historical)
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (18, 5, '2025-09-15', '/api/v1/users', 200, 90)");
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (19, 5, '2025-09-15', '/api/v1/health', 200, 25)");
    }

    /**
     * JOIN keys + clients, COUNT keys by status per client.
     * Acme Corp: active=1, revoked=1
     * HobbyDev: active=1, expired=1
     * StartupXYZ: active=1
     */
    public function testKeyStatusSummaryByClient(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.client_name, k.status, COUNT(*) AS key_count
             FROM mp_ak_api_clients c
             JOIN mp_ak_api_keys k ON c.id = k.client_id
             GROUP BY c.client_name, k.status
             ORDER BY c.client_name, k.status"
        );

        $this->assertCount(5, $rows);

        // Acme Corp: active=1
        $this->assertSame('Acme Corp', $rows[0]['client_name']);
        $this->assertSame('active', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['key_count']);

        // Acme Corp: revoked=1
        $this->assertSame('Acme Corp', $rows[1]['client_name']);
        $this->assertSame('revoked', $rows[1]['status']);
        $this->assertEquals(1, (int) $rows[1]['key_count']);

        // HobbyDev: active=1
        $this->assertSame('HobbyDev', $rows[2]['client_name']);
        $this->assertSame('active', $rows[2]['status']);
        $this->assertEquals(1, (int) $rows[2]['key_count']);

        // HobbyDev: expired=1
        $this->assertSame('HobbyDev', $rows[3]['client_name']);
        $this->assertSame('expired', $rows[3]['status']);
        $this->assertEquals(1, (int) $rows[3]['key_count']);

        // StartupXYZ: active=1
        $this->assertSame('StartupXYZ', $rows[4]['client_name']);
        $this->assertSame('active', $rows[4]['status']);
        $this->assertEquals(1, (int) $rows[4]['key_count']);
    }

    /**
     * GROUP BY key_id + request_date, COUNT requests per key per day.
     * Key 1: 2025-10-01=5, 2025-10-02=3
     * Key 3: 2025-10-01=4, 2025-10-02=2
     * Key 4: 2025-10-01=3
     * Key 5: 2025-09-15=2
     */
    public function testDailyUsageCountPerKey(): void
    {
        $rows = $this->ztdQuery(
            "SELECT key_id, request_date, COUNT(*) AS request_count
             FROM mp_ak_api_usage
             GROUP BY key_id, request_date
             ORDER BY key_id, request_date"
        );

        $this->assertCount(6, $rows);

        $this->assertEquals(1, (int) $rows[0]['key_id']);
        $this->assertSame('2025-10-01', $rows[0]['request_date']);
        $this->assertEquals(5, (int) $rows[0]['request_count']);

        $this->assertEquals(1, (int) $rows[1]['key_id']);
        $this->assertSame('2025-10-02', $rows[1]['request_date']);
        $this->assertEquals(3, (int) $rows[1]['request_count']);

        $this->assertEquals(3, (int) $rows[2]['key_id']);
        $this->assertSame('2025-10-01', $rows[2]['request_date']);
        $this->assertEquals(4, (int) $rows[2]['request_count']);

        $this->assertEquals(3, (int) $rows[3]['key_id']);
        $this->assertSame('2025-10-02', $rows[3]['request_date']);
        $this->assertEquals(2, (int) $rows[3]['request_count']);

        $this->assertEquals(4, (int) $rows[4]['key_id']);
        $this->assertSame('2025-10-01', $rows[4]['request_date']);
        $this->assertEquals(3, (int) $rows[4]['request_count']);

        $this->assertEquals(5, (int) $rows[5]['key_id']);
        $this->assertSame('2025-09-15', $rows[5]['request_date']);
        $this->assertEquals(2, (int) $rows[5]['request_count']);
    }

    /**
     * Daily requests vs daily_quota on 2025-10-01, ROUND percentage.
     * Acme Corp: 5 / 10000 = 0.05% (ROUND to 1 decimal = 0.1 or 0.0)
     * StartupXYZ: 4 / 1000 = 0.4%
     * HobbyDev: 3 / 100 = 3.0%
     */
    public function testQuotaUtilizationByClient(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.client_name, c.daily_quota,
                    COUNT(u.id) AS daily_requests,
                    ROUND(COUNT(u.id) * 100.0 / c.daily_quota, 1) AS utilization_pct
             FROM mp_ak_api_clients c
             JOIN mp_ak_api_keys k ON c.id = k.client_id
             JOIN mp_ak_api_usage u ON k.id = u.key_id AND u.request_date = '2025-10-01'
             GROUP BY c.client_name, c.daily_quota
             ORDER BY c.client_name"
        );

        $this->assertCount(3, $rows);

        // Acme Corp: 5 requests, 10000 quota
        $this->assertSame('Acme Corp', $rows[0]['client_name']);
        $this->assertEquals(5, (int) $rows[0]['daily_requests']);
        $this->assertEqualsWithDelta(0.1, (float) $rows[0]['utilization_pct'], 0.1);

        // HobbyDev: 3 requests, 100 quota
        $this->assertSame('HobbyDev', $rows[1]['client_name']);
        $this->assertEquals(3, (int) $rows[1]['daily_requests']);
        $this->assertEqualsWithDelta(3.0, (float) $rows[1]['utilization_pct'], 0.1);

        // StartupXYZ: 4 requests, 1000 quota
        $this->assertSame('StartupXYZ', $rows[2]['client_name']);
        $this->assertEquals(4, (int) $rows[2]['daily_requests']);
        $this->assertEqualsWithDelta(0.4, (float) $rows[2]['utilization_pct'], 0.1);
    }

    /**
     * SUM CASE for error (response_code >= 400) vs total, ROUND percentage.
     * /api/v1/orders: 1 error out of 5 = 20.0%
     * Others: 0%
     */
    public function testErrorRateByEndpoint(): void
    {
        $rows = $this->ztdQuery(
            "SELECT endpoint,
                    COUNT(*) AS total_requests,
                    SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) AS error_count,
                    ROUND(SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS error_rate
             FROM mp_ak_api_usage
             GROUP BY endpoint
             ORDER BY endpoint"
        );

        $this->assertCount(4, $rows);

        // /api/v1/health: 0 errors
        $this->assertSame('/api/v1/health', $rows[0]['endpoint']);
        $this->assertEquals(0, (int) $rows[0]['error_count']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['error_rate'], 0.1);

        // /api/v1/orders: 1 error out of 4
        $this->assertSame('/api/v1/orders', $rows[1]['endpoint']);
        $this->assertEquals(4, (int) $rows[1]['total_requests']);
        $this->assertEquals(1, (int) $rows[1]['error_count']);
        $this->assertEqualsWithDelta(25.0, (float) $rows[1]['error_rate'], 0.1);

        // /api/v1/products: 0 errors
        $this->assertSame('/api/v1/products', $rows[2]['endpoint']);
        $this->assertEquals(0, (int) $rows[2]['error_count']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[2]['error_rate'], 0.1);

        // /api/v1/users: 0 errors
        $this->assertSame('/api/v1/users', $rows[3]['endpoint']);
        $this->assertEquals(0, (int) $rows[3]['error_count']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[3]['error_rate'], 0.1);
    }

    /**
     * AVG response_time_ms grouped by tier.
     * enterprise: avg(45,52,120,88,12,38,250,95) = 87.5
     * pro: avg(65,78,82,15,55,70) = 60.8
     * free: avg(110,105,20,90,25) = 70.0
     */
    public function testAverageResponseTimeByTier(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.tier,
                    ROUND(AVG(u.response_time_ms), 1) AS avg_response_ms
             FROM mp_ak_api_clients c
             JOIN mp_ak_api_keys k ON c.id = k.client_id
             JOIN mp_ak_api_usage u ON k.id = u.key_id
             GROUP BY c.tier
             ORDER BY c.tier"
        );

        $this->assertCount(3, $rows);

        // enterprise: 87.5
        $this->assertSame('enterprise', $rows[0]['tier']);
        $this->assertEqualsWithDelta(87.5, (float) $rows[0]['avg_response_ms'], 0.1);

        // free: 70.0
        $this->assertSame('free', $rows[1]['tier']);
        $this->assertEqualsWithDelta(70.0, (float) $rows[1]['avg_response_ms'], 0.1);

        // pro: 60.8
        $this->assertSame('pro', $rows[2]['tier']);
        $this->assertEqualsWithDelta(60.8, (float) $rows[2]['avg_response_ms'], 0.1);
    }

    /**
     * Prepared statement lookup by key_prefix: 'sk_live_acme1' => 8 usage rows.
     */
    public function testPreparedUsageByKeyPrefix(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT u.id, u.request_date, u.endpoint, u.response_code, u.response_time_ms
             FROM mp_ak_api_usage u
             JOIN mp_ak_api_keys k ON u.key_id = k.id
             WHERE k.key_prefix = ?
             ORDER BY u.id",
            ['sk_live_acme1']
        );

        $this->assertCount(8, $rows);

        $this->assertSame('2025-10-01', $rows[0]['request_date']);
        $this->assertSame('/api/v1/users', $rows[0]['endpoint']);
        $this->assertEquals(200, (int) $rows[0]['response_code']);

        $this->assertSame('2025-10-02', $rows[7]['request_date']);
        $this->assertSame('/api/v1/orders', $rows[7]['endpoint']);
        $this->assertEquals(200, (int) $rows[7]['response_code']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_ak_api_usage VALUES (20, 1, '2025-10-03', '/api/v1/health', 200, 10)");

        // ZTD sees the new record
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ak_api_usage");
        $this->assertEquals(20, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_ak_api_usage")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
