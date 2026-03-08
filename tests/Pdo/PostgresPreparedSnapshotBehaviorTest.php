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
 * Tests prepared statement CTE snapshot behavior on PostgreSQL PDO.
 *
 * Cross-platform parity with SqlitePreparedSnapshotBehaviorTest.
 * @spec pending
 */
class PostgresPreparedSnapshotBehaviorTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_pgpsb_test');
        $raw->exec('CREATE TABLE pdo_pgpsb_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO pdo_pgpsb_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pdo_pgpsb_test VALUES (2, 'Bob', 80)");
    }

    /**
     * INSERT after prepare() is NOT visible.
     */
    public function testInsertAfterPrepareNotVisible(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_pgpsb_test');

        $this->pdo->exec("INSERT INTO pdo_pgpsb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Fresh prepare() after INSERT sees new data.
     */
    public function testFreshPrepareSeesNewData(): void
    {
        $this->pdo->exec("INSERT INTO pdo_pgpsb_test VALUES (3, 'Charlie', 90)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_pgpsb_test');
        $stmt->execute();
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Re-execution uses stale snapshot.
     */
    public function testReExecutionUsesStaleSnapshot(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_pgpsb_test');

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $this->pdo->exec("INSERT INTO pdo_pgpsb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_pgpsb_test');
        } catch (\Exception $e) {
        }
    }
}
