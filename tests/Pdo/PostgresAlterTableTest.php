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
 * Tests ALTER TABLE behavior in ZTD mode on PostgreSQL via PDO.
 *
 * PostgreSQL does NOT support ALTER TABLE in ZTD mode — it throws
 * ZtdPdoException ("ALTER TABLE not yet supported for PostgreSQL").
 */
class PostgresAlterTableTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_alter_test');
        $raw->exec('CREATE TABLE pg_alter_test (id INT PRIMARY KEY, name VARCHAR(255))');
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

    public function testAlterTableAddColumnThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_alter_test ADD COLUMN age INT');
    }

    public function testAlterTableDropColumnThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_alter_test DROP COLUMN name');
    }

    public function testAlterTableRenameColumnThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('ALTER TABLE pg_alter_test RENAME COLUMN name TO full_name');
    }

    public function testOriginalTableStillWorksAfterFailedAlter(): void
    {
        // ALTER TABLE throws, but shadow store remains functional
        try {
            $this->pdo->exec('ALTER TABLE pg_alter_test ADD COLUMN age INT');
        } catch (ZtdPdoException $e) {
            // expected
        }

        $this->pdo->exec("INSERT INTO pg_alter_test (id, name) VALUES (1, 'Alice')");
        $stmt = $this->pdo->query('SELECT * FROM pg_alter_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_alter_test');
    }
}
