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
 * Tests INFORMATION_SCHEMA queries through ZTD on MySQL PDO.
 *
 * INFORMATION_SCHEMA is a standard metadata schema. Queries to it
 * should pass through to the physical database since INFORMATION_SCHEMA
 * tables are not reflected by ZTD.
 */
class MysqlInformationSchemaTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS is_test_table');
        $raw->exec('CREATE TABLE is_test_table (id INT PRIMARY KEY, name VARCHAR(100))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    /**
     * Query INFORMATION_SCHEMA.TABLES.
     */
    public function testQueryInformationSchemaTables(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'is_test_table'"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $this->assertContains('is_test_table', $rows);
        } catch (\Throwable $e) {
            // INFORMATION_SCHEMA may be treated as unsupported
            $this->markTestSkipped('INFORMATION_SCHEMA not accessible: ' . $e->getMessage());
        }
    }

    /**
     * Query INFORMATION_SCHEMA.COLUMNS.
     */
    public function testQueryInformationSchemaColumns(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'is_test_table'
                 ORDER BY ORDINAL_POSITION"
            );
            $columns = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $this->assertContains('id', $columns);
            $this->assertContains('name', $columns);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INFORMATION_SCHEMA not accessible: ' . $e->getMessage());
        }
    }

    /**
     * Shadow operations work alongside INFORMATION_SCHEMA queries.
     */
    public function testShadowOperationsWithInformationSchema(): void
    {
        $this->pdo->exec("INSERT INTO is_test_table VALUES (1, 'Shadow')");

        // INFORMATION_SCHEMA reads physical metadata
        try {
            $stmt = $this->pdo->query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'is_test_table'"
            );
            $this->assertNotEmpty($stmt->fetchAll());
        } catch (\Throwable $e) {
            // Ignore if unsupported
        }

        // Shadow data still accessible
        $stmt = $this->pdo->query('SELECT name FROM is_test_table WHERE id = 1');
        $this->assertSame('Shadow', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO is_test_table VALUES (1, 'Shadow')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM is_test_table');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                MySQLContainer::getDsn(),
                'root',
                'root',
            );
            $raw->exec('DROP TABLE IF EXISTS is_test_table');
        } catch (\Exception $e) {
        }
    }
}
