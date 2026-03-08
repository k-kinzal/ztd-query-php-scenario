<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests ZtdPdo::connect() static factory method on MySQL (requires PHP 8.4+).
 * @spec SPEC-1.4a
 */
class MysqlConnectFactoryTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mcf_items');
        $raw->exec('CREATE TABLE mcf_items (id INT PRIMARY KEY, val VARCHAR(255))');
    }

    public function testConnectCreatesWorkingAdapter(): void
    {
        $pdo = ZtdPdo::connect(MySQLContainer::getDsn(), 'root', 'root');

        $pdo->exec("INSERT INTO mcf_items (id, val) VALUES (1, 'alpha')");

        $stmt = $pdo->query('SELECT val FROM mcf_items WHERE id = 1');
        $this->assertSame('alpha', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testConnectEnablesZtdByDefault(): void
    {
        $pdo = ZtdPdo::connect(MySQLContainer::getDsn(), 'root', 'root');
        $this->assertTrue($pdo->isZtdEnabled());
    }

    public function testConnectIsolatesShadowFromPhysical(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DELETE FROM mcf_items');
        $raw->exec("INSERT INTO mcf_items VALUES (1, 'physical')");

        $pdo = ZtdPdo::connect(MySQLContainer::getDsn(), 'root', 'root');

        // Shadow is empty — physical data not visible
        $stmt = $pdo->query('SELECT * FROM mcf_items');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->exec("INSERT INTO mcf_items (id, val) VALUES (2, 'shadow')");

        $stmt = $pdo->query('SELECT * FROM mcf_items');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        // Disable ZTD reveals physical data
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT val FROM mcf_items WHERE id = 1');
        $this->assertSame('physical', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testConnectSupportsUpdateAndDelete(): void
    {
        $pdo = ZtdPdo::connect(MySQLContainer::getDsn(), 'root', 'root');

        $pdo->exec("INSERT INTO mcf_items (id, val) VALUES (1, 'original')");
        $pdo->exec("INSERT INTO mcf_items (id, val) VALUES (2, 'keep')");

        $pdo->exec("UPDATE mcf_items SET val = 'modified' WHERE id = 1");
        $pdo->exec("DELETE FROM mcf_items WHERE id = 2");

        $stmt = $pdo->query('SELECT * FROM mcf_items ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('modified', $rows[0]['val']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mcf_items');
    }
}
