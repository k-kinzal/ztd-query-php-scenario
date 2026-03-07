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
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

class UnknownSchemaTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS late_table');
    }

    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): ZtdPdo
    {
        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);

        // Construct adapter BEFORE the table exists (schema not reflected)
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Now create the table physically
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('CREATE TABLE late_table (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO late_table VALUES (1, 'physical')");

        return $pdo;
    }

    public function testPassthroughUpdateOnUnknownTable(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // In passthrough mode, UPDATE on unknown table goes directly to MySQL
        $pdo->exec("UPDATE late_table SET val = 'updated' WHERE id = 1");

        // Verify the physical table was actually modified
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT val FROM late_table WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $rows[0]['val']);
    }

    public function testPassthroughDeleteOnUnknownTable(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // In passthrough mode, DELETE on unknown table goes directly to MySQL
        $pdo->exec("DELETE FROM late_table WHERE id = 1");

        // Verify the physical table row was actually deleted
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM late_table');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testExceptionUpdateOnUnknownTable(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
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

        // SELECT on unknown tables still passes through to MySQL
        $stmt = $pdo->query('SELECT * FROM late_table WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testInsertOnUnknownTableWorksInShadow(): void
    {
        $pdo = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        // INSERT on unknown tables can work in shadow mode
        $pdo->exec("INSERT INTO late_table (id, val) VALUES (2, 'shadow')");

        $stmt = $pdo->query('SELECT * FROM late_table WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);
    }

    protected function tearDown(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS late_table');
    }
}
