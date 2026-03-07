<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests ZtdPdo::fromPdo() behavior on MySQL.
 * Documents differences between fromPdo() and new ZtdPdo() constructor,
 * particularly around unknown schema handling.
 */
class MysqlFromPdoBehaviorTest extends TestCase
{
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
        $raw->exec('DROP TABLE IF EXISTS mfpb_items');
        $raw->exec('CREATE TABLE mfpb_items (id INT PRIMARY KEY, val VARCHAR(255))');
    }

    public function testFromPdoReflectsExistingSchema(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO mfpb_items (id, val) VALUES (1, 'hello')");
        $pdo->exec("UPDATE mfpb_items SET val = 'world' WHERE id = 1");

        $stmt = $pdo->query('SELECT val FROM mfpb_items WHERE id = 1');
        $this->assertSame('world', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testFromPdoUpdateOnUnreflectedTableThrows(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mfpb_late');

        // Wrap BEFORE creating the late table
        $pdo = ZtdPdo::fromPdo($raw);

        // Create the table after wrapping
        $raw->exec('CREATE TABLE mfpb_late (id INT PRIMARY KEY, val VARCHAR(255))');

        // INSERT works without schema info
        $pdo->exec("INSERT INTO mfpb_late (id, val) VALUES (1, 'test')");

        // UPDATE on unreflected table via fromPdo() throws RuntimeException
        // (unlike new ZtdPdo() constructor in Passthrough mode which passes through)
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE mfpb_late SET val = 'updated' WHERE id = 1");
    }

    public function testFromPdoIsolatesShadowFromPhysical(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DELETE FROM mfpb_items');
        $raw->exec("INSERT INTO mfpb_items VALUES (1, 'physical')");

        $pdo = ZtdPdo::fromPdo($raw);

        // Shadow is empty — physical data not visible
        $stmt = $pdo->query('SELECT * FROM mfpb_items');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Insert into shadow
        $pdo->exec("INSERT INTO mfpb_items (id, val) VALUES (2, 'shadow')");
        $stmt = $pdo->query('SELECT * FROM mfpb_items');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        // Disable ZTD reveals physical data
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM mfpb_items');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testFromPdoConstructorPassthroughDifference(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mfpb_late2');

        // new ZtdPdo() constructor in Passthrough mode
        $constructorPdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: new ZtdConfig(unknownSchemaBehavior: UnknownSchemaBehavior::Passthrough),
        );

        // Create late table
        $raw->exec('CREATE TABLE mfpb_late2 (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO mfpb_late2 VALUES (1, 'physical')");

        // Constructor-based: UPDATE passes through to physical DB
        $constructorPdo->exec("UPDATE mfpb_late2 SET val = 'constructor_updated' WHERE id = 1");

        // Verify physical update
        $constructorPdo->disableZtd();
        $stmt = $constructorPdo->query('SELECT val FROM mfpb_late2 WHERE id = 1');
        $this->assertSame('constructor_updated', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testFromPdoUpdateAfterShadowInsertThrows(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mfpb_late3');

        $fromPdo = ZtdPdo::fromPdo($raw);
        $raw->exec('CREATE TABLE mfpb_late3 (id INT PRIMARY KEY, val VARCHAR(255))');

        // Shadow INSERT registers the table in shadow store (without PK info)
        $fromPdo->exec("INSERT INTO mfpb_late3 (id, val) VALUES (1, 'shadow')");

        // UPDATE after shadow INSERT throws because table is now known but lacks PK schema
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $fromPdo->exec("UPDATE mfpb_late3 SET val = 'updated' WHERE id = 1");
    }

    public function testFromPdoPassthroughUpdateWithoutPriorShadowOps(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mfpb_late4');

        $fromPdo = ZtdPdo::fromPdo(
            $raw,
            new ZtdConfig(unknownSchemaBehavior: UnknownSchemaBehavior::Passthrough),
        );
        $raw->exec('CREATE TABLE mfpb_late4 (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO mfpb_late4 VALUES (1, 'physical')");

        // No prior shadow operations — Passthrough mode passes UPDATE to physical DB
        $fromPdo->exec("UPDATE mfpb_late4 SET val = 'pass_updated' WHERE id = 1");

        $fromPdo->disableZtd();
        $stmt = $fromPdo->query('SELECT val FROM mfpb_late4 WHERE id = 1');
        $this->assertSame('pass_updated', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testFromPdoSupportsPreparedStatements(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO mfpb_items (id, val) VALUES (1, 'a')");
        $pdo->exec("INSERT INTO mfpb_items (id, val) VALUES (2, 'b')");

        $stmt = $pdo->prepare('SELECT val FROM mfpb_items WHERE id = ?');
        $stmt->execute([2]);
        $this->assertSame('b', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mfpb_items');
        $raw->exec('DROP TABLE IF EXISTS mfpb_late');
        $raw->exec('DROP TABLE IF EXISTS mfpb_late2');
        $raw->exec('DROP TABLE IF EXISTS mfpb_late3');
        $raw->exec('DROP TABLE IF EXISTS mfpb_late4');
    }
}
