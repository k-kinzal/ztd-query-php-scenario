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
 * Tests the PostgreSQL ONLY keyword with ZTD.
 *
 * PostgreSQL supports ONLY in UPDATE ONLY, DELETE FROM ONLY, TRUNCATE ONLY
 * for table inheritance (excludes child tables). The PgSqlParser regex
 * patterns include (?:ONLY\s+)? to handle this keyword.
 *
 * In ZTD shadow mode, ONLY has no effect (no inheritance in shadow store),
 * but the parser should correctly extract table names when ONLY is present.
 */
class PostgresOnlyKeywordTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_only_test');
        $raw->exec('CREATE TABLE pg_only_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_only_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_only_test (id, name, score) VALUES (2, 'Bob', 80)");
    }

    public function testUpdateOnly(): void
    {
        $this->pdo->exec("UPDATE ONLY pg_only_test SET score = 95 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT score FROM pg_only_test WHERE id = 1');
        $this->assertEquals(95, $stmt->fetchColumn());
    }

    public function testDeleteFromOnly(): void
    {
        $this->pdo->exec("DELETE FROM ONLY pg_only_test WHERE id = 1");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_only_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    public function testTruncateOnly(): void
    {
        $this->pdo->exec("TRUNCATE ONLY pg_only_test");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_only_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public function testUpdateOnlyPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE ONLY pg_only_test SET name = 'Updated' WHERE id = 1");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_only_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_only_test');
    }
}
