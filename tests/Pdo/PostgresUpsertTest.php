<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class PostgresUpsertTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS upsert_test');
        $raw->exec('CREATE TABLE upsert_test (id INT PRIMARY KEY, val VARCHAR(255))');
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

    public function testInsertOnConflictDoUpdateInserts(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testInsertOnConflictDoUpdateUpdates(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('updated', $rows[0]['val']);
    }

    public function testInsertOnConflictDoNothingIgnoresDuplicate(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'ignored') ON CONFLICT (id) DO NOTHING");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('original', $rows[0]['val']);
    }

    public function testUpsertIsolation(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM upsert_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS upsert_test');
    }
}
