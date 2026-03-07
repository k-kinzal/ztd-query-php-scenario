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
 * Tests ZtdPdo::connect() static factory method on PostgreSQL (requires PHP 8.4+).
 */
class PostgresConnectFactoryTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pcf_items');
        $raw->exec('CREATE TABLE pcf_items (id INT PRIMARY KEY, val VARCHAR(255))');
    }

    public function testConnectCreatesWorkingAdapter(): void
    {
        $pdo = ZtdPdo::connect(PostgreSQLContainer::getDsn(), 'test', 'test');

        $pdo->exec("INSERT INTO pcf_items (id, val) VALUES (1, 'alpha')");

        $stmt = $pdo->query("SELECT val FROM pcf_items WHERE id = 1");
        $this->assertSame('alpha', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testConnectEnablesZtdByDefault(): void
    {
        $pdo = ZtdPdo::connect(PostgreSQLContainer::getDsn(), 'test', 'test');
        $this->assertTrue($pdo->isZtdEnabled());
    }

    public function testConnectIsolatesShadowFromPhysical(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DELETE FROM pcf_items');
        $raw->exec("INSERT INTO pcf_items VALUES (1, 'physical')");

        $pdo = ZtdPdo::connect(PostgreSQLContainer::getDsn(), 'test', 'test');

        $stmt = $pdo->query('SELECT * FROM pcf_items');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->exec("INSERT INTO pcf_items (id, val) VALUES (2, 'shadow')");

        $stmt = $pdo->query('SELECT * FROM pcf_items');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        $pdo->disableZtd();
        $stmt = $pdo->query("SELECT val FROM pcf_items WHERE id = 1");
        $this->assertSame('physical', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testConnectSupportsUpdateAndDelete(): void
    {
        $pdo = ZtdPdo::connect(PostgreSQLContainer::getDsn(), 'test', 'test');

        $pdo->exec("INSERT INTO pcf_items (id, val) VALUES (1, 'original')");
        $pdo->exec("INSERT INTO pcf_items (id, val) VALUES (2, 'keep')");

        $pdo->exec("UPDATE pcf_items SET val = 'modified' WHERE id = 1");
        $pdo->exec("DELETE FROM pcf_items WHERE id = 2");

        $stmt = $pdo->query('SELECT * FROM pcf_items ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('modified', $rows[0]['val']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pcf_items');
    }
}
