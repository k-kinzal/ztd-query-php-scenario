<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests rolling time-window rate limiting with token bucket patterns through ZTD shadow store (PostgreSQL PDO).
 * Covers request counting in time windows, rate limit checks with HAVING, top endpoints by client,
 * override application via COALESCE, tier usage aggregation, burst detection, and physical isolation.
 * @spec SPEC-10.2.119
 */
class PostgresSlidingWindowRateLimitTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rl_api_clients (
                id SERIAL PRIMARY KEY,
                client_name TEXT,
                tier TEXT,
                requests_per_hour INTEGER
            )',
            'CREATE TABLE pg_rl_api_requests (
                id SERIAL PRIMARY KEY,
                client_id INTEGER,
                endpoint TEXT,
                requested_at TEXT,
                response_status INTEGER
            )',
            'CREATE TABLE pg_rl_rate_limit_overrides (
                id SERIAL PRIMARY KEY,
                client_id INTEGER,
                endpoint TEXT,
                custom_limit INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rl_rate_limit_overrides', 'pg_rl_api_requests', 'pg_rl_api_clients'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 api clients
        $this->pdo->exec("INSERT INTO pg_rl_api_clients VALUES (1, 'acme-api', 'basic', 100)");
        $this->pdo->exec("INSERT INTO pg_rl_api_clients VALUES (2, 'bigcorp', 'pro', 500)");
        $this->pdo->exec("INSERT INTO pg_rl_api_clients VALUES (3, 'enterprise-co', 'enterprise', 2000)");

        // 10 api requests spanning a 1-hour window
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (1, 1, '/users', '2026-03-09 10:05:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (2, 1, '/users', '2026-03-09 10:10:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (3, 1, '/upload', '2026-03-09 10:12:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (4, 2, '/export', '2026-03-09 10:01:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (5, 2, '/export', '2026-03-09 10:02:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (6, 2, '/users', '2026-03-09 10:15:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (7, 3, '/users', '2026-03-09 10:20:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (8, 3, '/users', '2026-03-09 10:21:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (9, 3, '/data', '2026-03-09 10:22:00', 200)");
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (10, 1, '/users', '2026-03-09 10:03:00', 429)");

        // 2 rate limit overrides
        $this->pdo->exec("INSERT INTO pg_rl_rate_limit_overrides VALUES (1, 1, '/upload', 50)");
        $this->pdo->exec("INSERT INTO pg_rl_rate_limit_overrides VALUES (2, 2, '/export', 200)");
    }

    /**
     * COUNT requests for a specific client within a 1-hour sliding window using BETWEEN.
     * Client 1 (acme-api) has 4 requests between 10:00 and 11:00.
     */
    public function testRequestCountInWindow(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS req_count
             FROM pg_rl_api_requests
             WHERE client_id = 1
               AND requested_at BETWEEN '2026-03-09 10:00:00' AND '2026-03-09 11:00:00'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['req_count']);
    }

    /**
     * JOIN clients with their request counts in the window, use HAVING to find
     * clients exceeding a threshold. With threshold > 3, only client 1 (4 requests) qualifies.
     */
    public function testRateLimitCheck(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.client_name, COUNT(*) AS req_count, c.requests_per_hour
             FROM pg_rl_api_clients c
             JOIN pg_rl_api_requests r ON r.client_id = c.id
             WHERE r.requested_at BETWEEN '2026-03-09 10:00:00' AND '2026-03-09 11:00:00'
             GROUP BY c.id, c.client_name, c.requests_per_hour
             HAVING COUNT(*) > 3
             ORDER BY c.id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('acme-api', $rows[0]['client_name']);
        $this->assertEquals(4, (int) $rows[0]['req_count']);
        $this->assertEquals(100, (int) $rows[0]['requests_per_hour']);
    }

    /**
     * GROUP BY client_id, endpoint with COUNT to find the most-used endpoints per client.
     * Client 1: /users=3, /upload=1. Client 2: /export=2, /users=1. Client 3: /users=2, /data=1.
     */
    public function testTopEndpointsByClient(): void
    {
        $rows = $this->ztdQuery(
            "SELECT client_id, endpoint, COUNT(*) AS hit_count
             FROM pg_rl_api_requests
             GROUP BY client_id, endpoint
             ORDER BY client_id, hit_count DESC, endpoint"
        );

        $this->assertCount(6, $rows);

        // Client 1: /users=3
        $this->assertEquals(1, (int) $rows[0]['client_id']);
        $this->assertSame('/users', $rows[0]['endpoint']);
        $this->assertEquals(3, (int) $rows[0]['hit_count']);

        // Client 1: /upload=1
        $this->assertEquals(1, (int) $rows[1]['client_id']);
        $this->assertSame('/upload', $rows[1]['endpoint']);
        $this->assertEquals(1, (int) $rows[1]['hit_count']);

        // Client 2: /export=2
        $this->assertEquals(2, (int) $rows[2]['client_id']);
        $this->assertSame('/export', $rows[2]['endpoint']);
        $this->assertEquals(2, (int) $rows[2]['hit_count']);

        // Client 2: /users=1
        $this->assertEquals(2, (int) $rows[3]['client_id']);
        $this->assertSame('/users', $rows[3]['endpoint']);
        $this->assertEquals(1, (int) $rows[3]['hit_count']);

        // Client 3: /users=2
        $this->assertEquals(3, (int) $rows[4]['client_id']);
        $this->assertSame('/users', $rows[4]['endpoint']);
        $this->assertEquals(2, (int) $rows[4]['hit_count']);

        // Client 3: /data=1
        $this->assertEquals(3, (int) $rows[5]['client_id']);
        $this->assertSame('/data', $rows[5]['endpoint']);
        $this->assertEquals(1, (int) $rows[5]['hit_count']);
    }

    /**
     * LEFT JOIN rate_limit_overrides and use COALESCE to pick the effective limit.
     * acme-api /upload has override 50, bigcorp /export has override 200, others use default.
     */
    public function testOverrideApplied(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.client_name, r.endpoint,
                    COALESCE(o.custom_limit, c.requests_per_hour) AS effective_limit
             FROM pg_rl_api_clients c
             JOIN pg_rl_api_requests r ON r.client_id = c.id
             LEFT JOIN pg_rl_rate_limit_overrides o
                    ON o.client_id = c.id AND o.endpoint = r.endpoint
             GROUP BY c.id, c.client_name, r.endpoint, o.custom_limit, c.requests_per_hour
             ORDER BY c.id, r.endpoint"
        );

        $this->assertCount(6, $rows);

        // acme-api /upload: override = 50
        $upload = array_values(array_filter($rows, fn($r) => $r['client_name'] === 'acme-api' && $r['endpoint'] === '/upload'));
        $this->assertCount(1, $upload);
        $this->assertEquals(50, (int) $upload[0]['effective_limit']);

        // acme-api /users: no override, default = 100
        $users = array_values(array_filter($rows, fn($r) => $r['client_name'] === 'acme-api' && $r['endpoint'] === '/users'));
        $this->assertCount(1, $users);
        $this->assertEquals(100, (int) $users[0]['effective_limit']);

        // bigcorp /export: override = 200
        $export = array_values(array_filter($rows, fn($r) => $r['client_name'] === 'bigcorp' && $r['endpoint'] === '/export'));
        $this->assertCount(1, $export);
        $this->assertEquals(200, (int) $export[0]['effective_limit']);

        // bigcorp /users: no override, default = 500
        $busers = array_values(array_filter($rows, fn($r) => $r['client_name'] === 'bigcorp' && $r['endpoint'] === '/users'));
        $this->assertCount(1, $busers);
        $this->assertEquals(500, (int) $busers[0]['effective_limit']);
    }

    /**
     * GROUP BY tier with AVG, MAX, COUNT aggregation of request counts per tier.
     * basic: 1 client, 4 requests. pro: 1 client, 3 requests. enterprise: 1 client, 3 requests.
     */
    public function testTierUsageSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.tier,
                    COUNT(*) AS total_requests,
                    MAX(c.requests_per_hour) AS max_quota,
                    AVG(c.requests_per_hour) AS avg_quota
             FROM pg_rl_api_clients c
             JOIN pg_rl_api_requests r ON r.client_id = c.id
             GROUP BY c.tier
             ORDER BY c.tier"
        );

        $this->assertCount(3, $rows);

        // basic: 4 requests, quota 100
        $this->assertSame('basic', $rows[0]['tier']);
        $this->assertEquals(4, (int) $rows[0]['total_requests']);
        $this->assertEquals(100, (int) $rows[0]['max_quota']);

        // enterprise: 3 requests, quota 2000
        $this->assertSame('enterprise', $rows[1]['tier']);
        $this->assertEquals(3, (int) $rows[1]['total_requests']);
        $this->assertEquals(2000, (int) $rows[1]['max_quota']);

        // pro: 3 requests, quota 500
        $this->assertSame('pro', $rows[2]['tier']);
        $this->assertEquals(3, (int) $rows[2]['total_requests']);
        $this->assertEquals(500, (int) $rows[2]['max_quota']);
    }

    /**
     * Detect bursts: COUNT requests within a narrow 5-minute sub-window per client,
     * then use CASE to label as 'burst' (>= 2) or 'normal'.
     * In window 10:00-10:05: client 1 has 2 (ids 10,1), client 2 has 2 (ids 4,5).
     */
    public function testBurstDetection(): void
    {
        $rows = $this->ztdQuery(
            "SELECT client_id,
                    COUNT(*) AS window_count,
                    CASE WHEN COUNT(*) >= 2 THEN 'burst' ELSE 'normal' END AS burst_flag
             FROM pg_rl_api_requests
             WHERE requested_at BETWEEN '2026-03-09 10:00:00' AND '2026-03-09 10:05:00'
             GROUP BY client_id
             ORDER BY client_id"
        );

        $this->assertCount(2, $rows);

        // Client 1: 2 requests in 5-min window => burst
        $this->assertEquals(1, (int) $rows[0]['client_id']);
        $this->assertEquals(2, (int) $rows[0]['window_count']);
        $this->assertSame('burst', $rows[0]['burst_flag']);

        // Client 2: 2 requests in 5-min window => burst
        $this->assertEquals(2, (int) $rows[1]['client_id']);
        $this->assertEquals(2, (int) $rows[1]['window_count']);
        $this->assertSame('burst', $rows[1]['burst_flag']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_rl_api_requests VALUES (11, 1, '/health', '2026-03-09 10:30:00', 200)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_rl_api_requests");
        $this->assertSame(11, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT endpoint FROM pg_rl_api_requests WHERE id = 11");
        $this->assertSame('/health', $rows[0]['endpoint']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_rl_api_requests")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
