<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests COLLATE clause in queries with shadow data on MySQL.
 *
 * COLLATE affects string comparison and ordering behavior.
 * Tests whether ZTD CTE rewriting preserves COLLATE semantics.
 */
class MysqlCollateInQueryTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_collate_test');
        $raw->exec('CREATE TABLE pdo_collate_test (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            code VARCHAR(20)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_collate_test VALUES (1, 'Alice', 'abc')");
        $this->pdo->exec("INSERT INTO pdo_collate_test VALUES (2, 'alice', 'ABC')");
        $this->pdo->exec("INSERT INTO pdo_collate_test VALUES (3, 'Bob', 'def')");
        $this->pdo->exec("INSERT INTO pdo_collate_test VALUES (4, 'CHARLIE', 'GHI')");
        $this->pdo->exec("INSERT INTO pdo_collate_test VALUES (5, 'charlie', 'ghi')");
    }

    /**
     * WHERE with COLLATE for case-sensitive comparison.
     */
    public function testWhereCollateBinary(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pdo_collate_test WHERE name COLLATE utf8mb4_bin = 'alice'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Case-sensitive: only lowercase 'alice'
        $this->assertCount(1, $rows);
        $this->assertSame('alice', $rows[0]);
    }

    /**
     * WHERE with default (case-insensitive) collation.
     */
    public function testWhereDefaultCaseInsensitive(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pdo_collate_test WHERE name = 'alice' ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Case-insensitive (utf8mb4_general_ci): both 'Alice' and 'alice'
        $this->assertCount(2, $rows);
    }

    /**
     * ORDER BY with COLLATE for case-sensitive sorting.
     */
    public function testOrderByCollateBinary(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pdo_collate_test ORDER BY name COLLATE utf8mb4_bin");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Binary sort: uppercase before lowercase (A < B < C < a < b < c)
        $this->assertSame('Alice', $rows[0]);
        $this->assertSame('Bob', $rows[1]);
        $this->assertSame('CHARLIE', $rows[2]);
        $this->assertSame('alice', $rows[3]);
        $this->assertSame('charlie', $rows[4]);
    }

    /**
     * LIKE with COLLATE for case-sensitive pattern match.
     */
    public function testLikeWithCollateBinary(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pdo_collate_test WHERE name COLLATE utf8mb4_bin LIKE 'a%'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('alice', $rows[0]);
    }

    /**
     * COLLATE in WHERE after mutation.
     */
    public function testCollateInWhereAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_collate_test VALUES (6, 'ALICE', 'xyz')");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_collate_test WHERE name COLLATE utf8mb4_bin = 'ALICE'");
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(1, $count);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_collate_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_collate_test');
        } catch (\Exception $e) {
        }
    }
}
