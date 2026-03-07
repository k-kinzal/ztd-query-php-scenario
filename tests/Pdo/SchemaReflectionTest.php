<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

class SchemaReflectionTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS reflect_test');
    }

    public function testAdapterConstructedAfterTableReflectsSchema(): void
    {
        // Create table first
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('CREATE TABLE reflect_test (id INT PRIMARY KEY, val VARCHAR(255))');

        // Adapter constructed AFTER table exists → schema reflected
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
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
        // Construct adapter BEFORE table exists → schema NOT reflected
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        // Create table after adapter
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('CREATE TABLE reflect_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO reflect_test VALUES (1, 'physical')");

        // INSERT works (doesn't need primary key info)
        $pdo->exec("INSERT INTO reflect_test (id, val) VALUES (2, 'shadow')");

        // UPDATE fails because schema was not reflected (requires primary keys)
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/requires primary keys/i');
        $pdo->exec("UPDATE reflect_test SET val = 'updated' WHERE id = 1");
    }

    protected function tearDown(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS reflect_test');
    }
}
