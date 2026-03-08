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
 * Tests COLLATE clause in queries with shadow data on PostgreSQL.
 *
 * PostgreSQL uses different collation names than MySQL.
 * Tests whether COLLATE works correctly in CTE-rewritten shadow queries.
 */
class PostgresCollateInQueryTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_collate_test');
        $raw->exec('CREATE TABLE pg_collate_test (id INT PRIMARY KEY, name VARCHAR(50), code VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');

        $this->pdo->exec("INSERT INTO pg_collate_test VALUES (1, 'Alice', 'abc')");
        $this->pdo->exec("INSERT INTO pg_collate_test VALUES (2, 'alice', 'ABC')");
        $this->pdo->exec("INSERT INTO pg_collate_test VALUES (3, 'Bob', 'def')");
        $this->pdo->exec("INSERT INTO pg_collate_test VALUES (4, 'CHARLIE', 'GHI')");
        $this->pdo->exec("INSERT INTO pg_collate_test VALUES (5, 'charlie', 'ghi')");
    }

    /**
     * ORDER BY with COLLATE "C" for byte-order sorting.
     */
    public function testOrderByCollateC(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pg_collate_test ORDER BY name COLLATE "C"');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // "C" collation: uppercase letters sort before lowercase (A < B < C < a < b < c)
        $this->assertSame('Alice', $rows[0]);
        $this->assertSame('Bob', $rows[1]);
        $this->assertSame('CHARLIE', $rows[2]);
        $this->assertSame('alice', $rows[3]);
        $this->assertSame('charlie', $rows[4]);
    }

    /**
     * WHERE with COLLATE "C" for case-sensitive comparison.
     */
    public function testWhereCollateC(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_collate_test WHERE name COLLATE \"C\" = 'alice'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('alice', $rows[0]);
    }

    /**
     * COLLATE after mutation.
     */
    public function testCollateAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO pg_collate_test VALUES (6, 'ALICE', 'xyz')");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pg_collate_test WHERE name COLLATE \"C\" = 'ALICE'");
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_collate_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_collate_test');
        } catch (\Exception $e) {
        }
    }
}
