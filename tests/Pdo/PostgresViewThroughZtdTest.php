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
 * Tests database views behavior through ZTD on PostgreSQL PDO.
 * @spec SPEC-3.3b
 */
class PostgresViewThroughZtdTest extends TestCase
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
        $raw->exec('DROP VIEW IF EXISTS pdo_pg_vtzt_active');
        $raw->exec('DROP TABLE IF EXISTS pdo_pg_vtzt_users');
        $raw->exec('CREATE TABLE pdo_pg_vtzt_users (id INT PRIMARY KEY, name VARCHAR(50), active INT)');
        $raw->exec("INSERT INTO pdo_pg_vtzt_users VALUES (1, 'Alice', 1)");
        $raw->exec("INSERT INTO pdo_pg_vtzt_users VALUES (2, 'Bob', 0)");
        $raw->exec("INSERT INTO pdo_pg_vtzt_users VALUES (3, 'Charlie', 1)");
        $raw->exec('CREATE VIEW pdo_pg_vtzt_active AS SELECT id, name FROM pdo_pg_vtzt_users WHERE active = 1');
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
    }

    /**
     * View returns physical data.
     */
    public function testViewReturnsPhysicalData(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pg_vtzt_active');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow mutations on base table not visible through view.
     */
    public function testShadowMutationsNotVisibleThroughView(): void
    {
        $this->pdo->exec("INSERT INTO pdo_pg_vtzt_users VALUES (4, 'Diana', 1)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pg_vtzt_active');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation of base table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pg_vtzt_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP VIEW IF EXISTS pdo_pg_vtzt_active');
            $raw->exec('DROP TABLE IF EXISTS pdo_pg_vtzt_users');
        } catch (\Exception $e) {
        }
    }
}
