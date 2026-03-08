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
 * Tests DELETE without WHERE clause on PostgreSQL PDO.
 *
 * Like MySQL, PostgreSQL correctly clears the shadow store.
 */
class PostgresDeleteWithoutWhereTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_pgdww_test');
        $raw->exec('CREATE TABLE pdo_pgdww_test (id INT PRIMARY KEY, name VARCHAR(50))');
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

        $this->pdo->exec("INSERT INTO pdo_pgdww_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_pgdww_test VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_pgdww_test VALUES (3, 'Charlie')");
    }

    /**
     * DELETE without WHERE works correctly on PostgreSQL.
     */
    public function testDeleteWithoutWhereWorks(): void
    {
        $this->pdo->exec('DELETE FROM pdo_pgdww_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE with WHERE 1=1 also works.
     */
    public function testDeleteWithWhereTrueWorks(): void
    {
        $this->pdo->exec('DELETE FROM pdo_pgdww_test WHERE 1=1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('DELETE FROM pdo_pgdww_test');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_pgdww_test');
        } catch (\Exception $e) {
        }
    }
}
