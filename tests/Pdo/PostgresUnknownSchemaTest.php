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
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

class PostgresUnknownSchemaTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS late_table');
    }

    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): ZtdPdo
    {
        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);

        // Construct adapter BEFORE the table exists (schema not reflected)
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Now create the table physically
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('CREATE TABLE late_table (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO late_table VALUES (1, 'physical')");

        return $pdo;
    }

    public function testPassthroughUpdateOnUnknownTableThrowsRuntimeException(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // PostgreSQL PDO adapter throws RuntimeException ("UPDATE simulation requires primary keys")
        // instead of passing through like the MySQL PDO adapter.
        // This is a behavioral difference between MySQL and PostgreSQL PDO adapters.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_table SET val = 'updated' WHERE id = 1");
    }

    public function testPassthroughDeleteOnUnknownTable(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $pdo->exec("DELETE FROM late_table WHERE id = 1");

        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM late_table');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testExceptionUpdateOnUnknownTableThrowsRuntimeException(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        // PostgreSQL throws RuntimeException from ShadowStore rather than ZtdPdoException.
        // This differs from MySQL which throws ZtdPdoException("Unknown table").
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_table SET val = 'updated' WHERE id = 1");
    }

    public function testExceptionDeleteOnUnknownTable(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
        $pdo->exec("DELETE FROM late_table WHERE id = 1");
    }

    public function testSelectOnUnknownTablePassesThrough(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $stmt = $pdo->query('SELECT * FROM late_table WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testInsertOnUnknownTableWorksInShadow(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $pdo->exec("INSERT INTO late_table (id, val) VALUES (2, 'shadow')");

        $stmt = $pdo->query('SELECT * FROM late_table WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);
    }

    protected function tearDown(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS late_table');
    }
}
