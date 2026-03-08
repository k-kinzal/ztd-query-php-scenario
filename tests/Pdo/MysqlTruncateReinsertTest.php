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
 * Tests TRUNCATE TABLE + re-insert workflow on MySQL PDO.
 *
 * TRUNCATE clears the shadow store, then new INSERTs populate cleanly.
 */
class MysqlTruncateReinsertTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_mtr_test');
        $raw->exec('CREATE TABLE pdo_mtr_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (2, 'Bob')");
    }

    /**
     * TRUNCATE clears shadow store.
     */
    public function testTruncateClearsShadow(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT after TRUNCATE works.
     */
    public function testInsertAfterTruncate(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');

        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Charlie')");

        $stmt = $this->pdo->query('SELECT name FROM pdo_mtr_test WHERE id = 1');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    /**
     * Multiple truncate-reinsert cycles work.
     */
    public function testMultipleTruncateCycles(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Round1')");

        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'Round2')");

        $stmt = $this->pdo->query('SELECT name FROM pdo_mtr_test WHERE id = 1');
        $this->assertSame('Round2', $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtr_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation after TRUNCATE.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_mtr_test');
        $this->pdo->exec("INSERT INTO pdo_mtr_test VALUES (1, 'New')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_mtr_test');
        } catch (\Exception $e) {
        }
    }
}
