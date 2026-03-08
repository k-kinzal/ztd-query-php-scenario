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
 * Tests prepared statement CTE snapshot behavior on MySQL PDO.
 *
 * Cross-platform parity with SqlitePreparedSnapshotBehaviorTest.
 */
class MysqlPreparedSnapshotBehaviorTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_mpsb_test');
        $raw->exec('CREATE TABLE pdo_mpsb_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (2, 'Bob', 80)");
    }

    /**
     * INSERT after prepare() is NOT visible.
     */
    public function testInsertAfterPrepareNotVisible(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_mpsb_test');

        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Fresh prepare() after INSERT sees new data.
     */
    public function testFreshPrepareSeesNewData(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (3, 'Charlie', 90)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_mpsb_test');
        $stmt->execute();
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Re-execution uses stale snapshot.
     */
    public function testReExecutionUsesStaleSnapshot(): void
    {
        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM pdo_mpsb_test');

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $this->pdo->exec("INSERT INTO pdo_mpsb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_mpsb_test');
        } catch (\Exception $e) {
        }
    }
}
