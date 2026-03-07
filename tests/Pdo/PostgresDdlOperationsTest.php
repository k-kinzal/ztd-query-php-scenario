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

class PostgresDdlOperationsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS ddl_existing');
        $raw->exec('CREATE TABLE ddl_existing (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec('DROP TABLE IF EXISTS ddl_new');
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

    public function testCreateTableThrowsWhenTableExistsPhysically(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/already exists/i');

        $this->pdo->exec('CREATE TABLE ddl_existing (id INT PRIMARY KEY)');
    }

    public function testCreateTableInShadowWhenNotPhysical(): void
    {
        $this->pdo->exec('CREATE TABLE ddl_new (id INT PRIMARY KEY, name VARCHAR(255))');

        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (1, 'shadow')");

        $stmt = $this->pdo->query('SELECT * FROM ddl_new WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame('shadow', $rows[0]['name']);
    }

    public function testDropTableClearsShadowData(): void
    {
        $this->pdo->exec("INSERT INTO ddl_existing (id, val) VALUES (1, 'shadow')");

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        $this->pdo->exec('DROP TABLE ddl_existing');

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testTruncateClearsShadowedData(): void
    {
        $this->pdo->exec("INSERT INTO ddl_existing (id, val) VALUES (1, 'a')");
        $this->pdo->exec("INSERT INTO ddl_existing (id, val) VALUES (2, 'b')");

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing');
        $this->assertCount(2, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $this->pdo->exec('TRUNCATE TABLE ddl_existing');

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testUpdateOnShadowCreatedTable(): void
    {
        $this->pdo->exec('CREATE TABLE ddl_new (id INT PRIMARY KEY, name VARCHAR(255))');
        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (1, 'original')");
        $this->pdo->exec("UPDATE ddl_new SET name = 'updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM ddl_new WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $rows[0]['name']);
    }

    public function testDeleteOnShadowCreatedTable(): void
    {
        $this->pdo->exec('CREATE TABLE ddl_new (id INT PRIMARY KEY, name VARCHAR(255))');
        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (2, 'world')");
        $this->pdo->exec('DELETE FROM ddl_new WHERE id = 1');

        $stmt = $this->pdo->query('SELECT * FROM ddl_new');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS ddl_existing');
        $raw->exec('DROP TABLE IF EXISTS ddl_new');
    }
}
