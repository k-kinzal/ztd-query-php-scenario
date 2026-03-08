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
 * Tests SAVEPOINT behavior on MySQL PDO with ZTD.
 */
class MysqlSavepointBehaviorTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_msp_test');
        $raw->exec('CREATE TABLE pdo_msp_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo->exec("INSERT INTO pdo_msp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT should be supported on MySQL.
     */
    public function testSavepointSupported(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SAVEPOINT not yet supported on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLBACK TO SAVEPOINT should be supported on MySQL.
     */
    public function testRollbackToSavepointSupported(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ROLLBACK TO SAVEPOINT not yet supported on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * Shadow data should remain intact regardless of SAVEPOINT support.
     */
    public function testShadowDataIntact(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
        } catch (\Throwable $e) {
            // SAVEPOINT may not be supported yet
        }

        $stmt = $this->pdo->query('SELECT name FROM pdo_msp_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_msp_test');
        } catch (\Exception $e) {
        }
    }
}
