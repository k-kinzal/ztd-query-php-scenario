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
 *
 * On MySQL, SAVEPOINT and RELEASE SAVEPOINT throw errors.
 * ROLLBACK TO SAVEPOINT throws "Statement type not supported."
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
     * SAVEPOINT throws on MySQL.
     */
    public function testSavepointThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('SAVEPOINT sp1');
    }

    /**
     * ROLLBACK TO SAVEPOINT throws on MySQL.
     */
    public function testRollbackToSavepointThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
    }

    /**
     * Shadow data unaffected by failed SAVEPOINT.
     */
    public function testShadowDataUnaffected(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
        } catch (\Throwable $e) {
            // Expected
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
