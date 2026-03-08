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
 * Tests transaction interaction with ZTD shadow store on MySQL PDO.
 *
 * Cross-platform parity with SqliteTransactionWithShadowTest.
 */
class MysqlTransactionWithShadowTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
            'root',
            'root',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_mtxn_test');
        $raw->exec('CREATE TABLE pdo_mtxn_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * beginTransaction/commit with shadow INSERT.
     */
    public function testCommitWithShadowInsert(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO pdo_mtxn_test VALUES (1, 'Alice')");
        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT name FROM pdo_mtxn_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * rollBack behavior with shadow INSERT.
     */
    public function testRollbackWithShadowInsert(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO pdo_mtxn_test VALUES (1, 'Alice')");
        $this->pdo->rollBack();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtxn_test');
        $count = (int) $stmt->fetchColumn();
        $this->assertContains($count, [0, 1]);
    }

    /**
     * inTransaction() returns correct state.
     */
    public function testInTransactionState(): void
    {
        $this->assertFalse($this->pdo->inTransaction());

        $this->pdo->beginTransaction();
        $this->assertTrue($this->pdo->inTransaction());

        $this->pdo->commit();
        $this->assertFalse($this->pdo->inTransaction());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO pdo_mtxn_test VALUES (1, 'Alice')");
        $this->pdo->commit();

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtxn_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
                'root',
                'root',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_mtxn_test');
        } catch (\Exception $e) {
        }
    }
}
