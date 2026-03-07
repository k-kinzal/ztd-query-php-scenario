<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests CREATE TABLE LIKE, CREATE TABLE AS SELECT, and ALTER TABLE behavior on PostgreSQL.
 */
class PostgresCreateTableVariantsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_ctv_target');
        $raw->exec('DROP TABLE IF EXISTS pg_ctv_ctas');
        $raw->exec('DROP TABLE IF EXISTS pg_ctv_source');
        $raw->exec('CREATE TABLE pg_ctv_source (id INT PRIMARY KEY, val VARCHAR(255))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testCreateTableLike(): void
    {
        $this->pdo->exec('CREATE TABLE pg_ctv_target (LIKE pg_ctv_source)');

        $this->pdo->exec("INSERT INTO pg_ctv_target (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM pg_ctv_target WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelect(): void
    {
        $this->pdo->exec("INSERT INTO pg_ctv_source (id, val) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO pg_ctv_source (id, val) VALUES (2, 'world')");

        $this->pdo->exec('CREATE TABLE pg_ctv_ctas AS SELECT * FROM pg_ctv_source');

        $stmt = $this->pdo->query('SELECT * FROM pg_ctv_ctas ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('hello', $rows[0]['val']);
        $this->assertSame('world', $rows[1]['val']);
    }

    public function testAlterTableThrowsOnPostgresql(): void
    {
        // ALTER TABLE is not supported on PostgreSQL — throws ZtdPdoException
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_ctv_source ADD COLUMN extra VARCHAR(100)');
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_ctv_target');
        $raw->exec('DROP TABLE IF EXISTS pg_ctv_ctas');
        $raw->exec('DROP TABLE IF EXISTS pg_ctv_source');
    }
}
