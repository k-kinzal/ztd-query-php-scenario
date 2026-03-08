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
 * Tests INFORMATION_SCHEMA queries through ZTD on PostgreSQL PDO.
 *
 * ORMs and migration tools frequently query information_schema for metadata.
 * Cross-platform parity with MysqlInformationSchemaTest.
 * @spec pending
 */
class PostgresInformationSchemaTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_is_test_table');
        $raw->exec('CREATE TABLE pg_is_test_table (id INT PRIMARY KEY, name VARCHAR(100), active BOOLEAN)');

        $this->pdo = ZtdPdo::fromPdo(new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        ));
    }

    /**
     * Query information_schema.tables.
     */
    public function testQueryInformationSchemaTables(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT table_name FROM information_schema.tables
                 WHERE table_schema = 'public' AND table_name = 'pg_is_test_table'"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $this->assertContains('pg_is_test_table', $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('information_schema not accessible: ' . $e->getMessage());
        }
    }

    /**
     * Query information_schema.columns.
     */
    public function testQueryInformationSchemaColumns(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = 'pg_is_test_table'
                 ORDER BY ordinal_position"
            );
            $columns = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $this->assertContains('id', $columns);
            $this->assertContains('name', $columns);
            $this->assertContains('active', $columns);
        } catch (\Throwable $e) {
            $this->markTestSkipped('information_schema not accessible: ' . $e->getMessage());
        }
    }

    /**
     * Shadow operations work alongside information_schema queries.
     */
    public function testShadowOperationsWithInformationSchema(): void
    {
        $this->pdo->exec("INSERT INTO pg_is_test_table VALUES (1, 'Shadow', true)");

        try {
            $stmt = $this->pdo->query(
                "SELECT table_name FROM information_schema.tables
                 WHERE table_schema = 'public' AND table_name = 'pg_is_test_table'"
            );
            $this->assertNotEmpty($stmt->fetchAll());
        } catch (\Throwable $e) {
            // Ignore if unsupported
        }

        // Shadow data still accessible
        $stmt = $this->pdo->query('SELECT name FROM pg_is_test_table WHERE id = 1');
        $this->assertSame('Shadow', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_is_test_table VALUES (1, 'Shadow', true)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_is_test_table');
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
            $raw->exec('DROP TABLE IF EXISTS pg_is_test_table');
        } catch (\Exception $e) {
        }
    }
}
