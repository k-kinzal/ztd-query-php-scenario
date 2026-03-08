<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests ZtdPdo::fromPdo() behavior on PostgreSQL.
 * Documents differences between fromPdo() and new ZtdPdo() constructor.
 * @spec SPEC-1.4
 */
class PostgresFromPdoBehaviorTest extends TestCase
{
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
        $raw->exec('DROP TABLE IF EXISTS pfpb_items');
        $raw->exec('CREATE TABLE pfpb_items (id INT PRIMARY KEY, val VARCHAR(255))');
    }

    public function testFromPdoReflectsExistingSchema(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO pfpb_items (id, val) VALUES (1, 'hello')");
        $pdo->exec("UPDATE pfpb_items SET val = 'world' WHERE id = 1");

        $stmt = $pdo->query("SELECT val FROM pfpb_items WHERE id = 1");
        $this->assertSame('world', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testFromPdoUpdateOnUnreflectedTableThrows(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pfpb_late');

        $pdo = ZtdPdo::fromPdo($raw);

        $raw->exec('CREATE TABLE pfpb_late (id INT PRIMARY KEY, val VARCHAR(255))');

        $pdo->exec("INSERT INTO pfpb_late (id, val) VALUES (1, 'test')");

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE pfpb_late SET val = 'updated' WHERE id = 1");
    }

    public function testFromPdoIsolatesShadowFromPhysical(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DELETE FROM pfpb_items');
        $raw->exec("INSERT INTO pfpb_items VALUES (1, 'physical')");

        $pdo = ZtdPdo::fromPdo($raw);

        // Shadow is empty
        $stmt = $pdo->query('SELECT * FROM pfpb_items');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->exec("INSERT INTO pfpb_items (id, val) VALUES (2, 'shadow')");
        $stmt = $pdo->query('SELECT * FROM pfpb_items');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM pfpb_items');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testFromPdoSupportsPreparedStatements(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO pfpb_items (id, val) VALUES (1, 'a')");
        $pdo->exec("INSERT INTO pfpb_items (id, val) VALUES (2, 'b')");

        $stmt = $pdo->prepare('SELECT val FROM pfpb_items WHERE id = ?');
        $stmt->execute([2]);
        $this->assertSame('b', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testFromPdoSupportsJoins(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pfpb_child');
        $raw->exec('DROP TABLE IF EXISTS pfpb_parent');
        $raw->exec('CREATE TABLE pfpb_parent (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE pfpb_child (id INT PRIMARY KEY, parent_id INT, label VARCHAR(50))');

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO pfpb_parent (id, name) VALUES (1, 'parent1')");
        $pdo->exec("INSERT INTO pfpb_child (id, parent_id, label) VALUES (1, 1, 'child1')");

        $stmt = $pdo->query('SELECT p.name, c.label FROM pfpb_parent p JOIN pfpb_child c ON c.parent_id = p.id');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('parent1', $row['name']);
        $this->assertSame('child1', $row['label']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pfpb_items');
        $raw->exec('DROP TABLE IF EXISTS pfpb_late');
        $raw->exec('DROP TABLE IF EXISTS pfpb_child');
        $raw->exec('DROP TABLE IF EXISTS pfpb_parent');
    }
}
