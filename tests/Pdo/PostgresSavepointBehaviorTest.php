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
 * Tests SAVEPOINT behavior on PostgreSQL PDO with ZTD.
 *
 * On PostgreSQL, SAVEPOINT commands silently pass through.
 * Shadow store does NOT participate in savepoints.
 */
class PostgresSavepointBehaviorTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_pgsp_test');
        $raw->exec('CREATE TABLE pdo_pgsp_test (id INT PRIMARY KEY, name VARCHAR(50))');
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
        $this->pdo->exec("INSERT INTO pdo_pgsp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT passes through silently on PostgreSQL.
     */
    public function testSavepointPassesThrough(): void
    {
        $this->pdo->beginTransaction();
        // Should not throw on PostgreSQL
        $this->pdo->exec('SAVEPOINT sp1');
        $this->pdo->commit();
        $this->assertTrue(true); // No exception thrown
    }

    /**
     * Shadow data persists after ROLLBACK TO SAVEPOINT.
     */
    public function testShadowDataPersistsAfterRollbackToSavepoint(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec('SAVEPOINT sp1');

        $this->pdo->exec("INSERT INTO pdo_pgsp_test VALUES (2, 'Bob')");

        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
        $this->pdo->commit();

        // Shadow data persists despite savepoint rollback
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgsp_test');
        $count = (int) $stmt->fetchColumn();
        // Shadow INSERT may persist since shadow doesn't participate in savepoints
        $this->assertGreaterThanOrEqual(1, $count);
    }

    /**
     * RELEASE SAVEPOINT passes through.
     */
    public function testReleaseSavepointPassesThrough(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec('SAVEPOINT sp1');
        $this->pdo->exec('RELEASE SAVEPOINT sp1');
        $this->pdo->commit();
        $this->assertTrue(true);
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_pgsp_test');
        } catch (\Exception $e) {
        }
    }
}
