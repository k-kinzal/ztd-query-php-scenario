<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class PostgresSchemaReflectionTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS reflect_test');
    }

    public function testAdapterConstructedAfterTableReflectsSchema(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('CREATE TABLE reflect_test (id INT PRIMARY KEY, val VARCHAR(255))');

        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo->exec("INSERT INTO reflect_test (id, val) VALUES (1, 'original')");
        $pdo->exec("UPDATE reflect_test SET val = 'updated' WHERE id = 1");

        $stmt = $pdo->query('SELECT val FROM reflect_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $rows[0]['val']);

        $pdo->exec("DELETE FROM reflect_test WHERE id = 1");
        $stmt = $pdo->query('SELECT * FROM reflect_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testUpdateFailsWhenSchemaNotReflected(): void
    {
        // Construct adapter BEFORE table exists
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('CREATE TABLE reflect_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO reflect_test VALUES (1, 'physical')");

        // INSERT works (doesn't need primary key info)
        $pdo->exec("INSERT INTO reflect_test (id, val) VALUES (2, 'shadow')");

        // UPDATE fails because schema was not reflected
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/requires primary keys/i');
        $pdo->exec("UPDATE reflect_test SET val = 'updated' WHERE id = 1");
    }

    protected function tearDown(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS reflect_test');
    }
}
