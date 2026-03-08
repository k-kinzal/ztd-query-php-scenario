<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests multi-tenant query patterns on PostgreSQL PDO.
 *
 * Cross-platform parity with MultiTenantPatternTest (MySQLi).
 * @spec SPEC-10.2.21
 */
class PostgresMultiTenantPatternTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_mt_projects');
        $raw->exec('DROP TABLE IF EXISTS pg_mt_users');
        $raw->exec('CREATE TABLE pg_mt_users (id INT PRIMARY KEY, tenant_id INT NOT NULL, name VARCHAR(50), role VARCHAR(20))');
        $raw->exec('CREATE TABLE pg_mt_projects (id INT PRIMARY KEY, tenant_id INT NOT NULL, name VARCHAR(50), owner_id INT)');

        $this->pdo = ZtdPdo::fromPdo(new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        ));

        $this->pdo->exec("INSERT INTO pg_mt_users VALUES (1, 1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO pg_mt_users VALUES (2, 1, 'Bob', 'member')");
        $this->pdo->exec("INSERT INTO pg_mt_users VALUES (3, 2, 'Charlie', 'admin')");
        $this->pdo->exec("INSERT INTO pg_mt_users VALUES (4, 2, 'Diana', 'member')");
        $this->pdo->exec("INSERT INTO pg_mt_users VALUES (5, 2, 'Eve', 'member')");
        $this->pdo->exec("INSERT INTO pg_mt_projects VALUES (1, 1, 'Project Alpha', 1)");
        $this->pdo->exec("INSERT INTO pg_mt_projects VALUES (2, 1, 'Project Beta', 2)");
        $this->pdo->exec("INSERT INTO pg_mt_projects VALUES (3, 2, 'Project Gamma', 3)");
    }

    public function testSelectByTenant(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pg_mt_users WHERE tenant_id = 1 ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testPreparedSelectByTenant(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pg_mt_users WHERE tenant_id = ? ORDER BY name');
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    public function testInsertWithTenantIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_mt_users VALUES (6, 1, 'Frank', 'member')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mt_users WHERE tenant_id = 1');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mt_users WHERE tenant_id = 2');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testUpdateByTenant(): void
    {
        $this->pdo->exec("UPDATE pg_mt_users SET role = 'viewer' WHERE tenant_id = 2 AND role = 'member'");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pg_mt_users WHERE tenant_id = 2 AND role = 'viewer'");
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT role FROM pg_mt_users WHERE id = 2');
        $this->assertSame('member', $stmt->fetchColumn());
    }

    public function testDeleteByTenant(): void
    {
        $this->pdo->exec("DELETE FROM pg_mt_users WHERE tenant_id = 2 AND role = 'member'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mt_users WHERE tenant_id = 2');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mt_users WHERE tenant_id = 1');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testJoinWithTenantFilter(): void
    {
        $stmt = $this->pdo->query(
            'SELECT p.name AS project, u.name AS owner
             FROM pg_mt_projects p
             JOIN pg_mt_users u ON p.owner_id = u.id
             WHERE p.tenant_id = 1
             ORDER BY p.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Project Alpha', $rows[0]['project']);
        $this->assertSame('Alice', $rows[0]['owner']);
    }

    public function testAggregationPerTenant(): void
    {
        $stmt = $this->pdo->query(
            'SELECT tenant_id, COUNT(*) AS user_count
             FROM pg_mt_users
             GROUP BY tenant_id
             ORDER BY tenant_id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertEquals(2, (int) $rows[0]['user_count']);
        $this->assertEquals(3, (int) $rows[1]['user_count']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mt_users');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pg_mt_projects');
            $raw->exec('DROP TABLE IF EXISTS pg_mt_users');
        } catch (\Exception $e) {
        }
    }
}
