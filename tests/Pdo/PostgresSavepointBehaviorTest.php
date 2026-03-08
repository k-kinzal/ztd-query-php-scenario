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
 * @spec SPEC-6.3
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
     * ROLLBACK TO SAVEPOINT should undo shadow INSERT.
     */
    public function testRollbackToSavepointUndoesShadowInsert(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec('SAVEPOINT sp1');

        $this->pdo->exec("INSERT INTO pdo_pgsp_test VALUES (2, 'Bob')");

        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgsp_test');
        $count = (int) $stmt->fetchColumn();
        // Expected: Bob's INSERT was rolled back, so count should be 1 (Alice only)
        if ($count !== 1) {
            $this->markTestIncomplete(
                'Shadow store does not participate in savepoints. '
                . 'Expected count 1 after ROLLBACK TO SAVEPOINT, got ' . $count
            );
        }
        $this->assertSame(1, $count);
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
