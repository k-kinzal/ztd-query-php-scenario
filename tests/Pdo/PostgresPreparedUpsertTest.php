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
 * Tests prepared statement UPSERT (ON CONFLICT DO UPDATE) on PostgreSQL PDO.
 *
 * PDO adapter limitation: prepared UPSERT does NOT update existing rows.
 */
class PostgresPreparedUpsertTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_upsert_test');
        $raw->exec('CREATE TABLE pdo_upsert_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testPreparedUpsertInsertsNewRow(): void
    {
        $stmt = $this->pdo->prepare(
            'INSERT INTO pdo_upsert_test (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score'
        );
        $stmt->execute([1, 'Alice', 100]);

        $select = $this->pdo->query('SELECT name, score FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * PDO limitation: prepared ON CONFLICT DO UPDATE does NOT update existing rows.
     */
    public function testPreparedUpsertDoesNotUpdateExisting(): void
    {
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare(
            'INSERT INTO pdo_upsert_test (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score'
        );
        $stmt->execute([1, 'Updated', 200]);

        $select = $this->pdo->query('SELECT name FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        // Bug: should be 'Updated' but old row is retained
        $this->assertSame('Original', $row['name']);
    }

    /**
     * Non-prepared UPSERT via exec() works correctly.
     */
    public function testExecUpsertWorksCorrectly(): void
    {
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Original', 50)");
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Updated', 200) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score");

        $select = $this->pdo->query('SELECT name, score FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Updated', $row['name']);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * Prepared ON CONFLICT DO NOTHING works (row not inserted, no error).
     */
    public function testPreparedOnConflictDoNothingNoError(): void
    {
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare(
            'INSERT INTO pdo_upsert_test (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING'
        );
        $stmt->execute([1, 'Ignored', 999]);

        $select = $this->pdo->query('SELECT name FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Original', $row['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_upsert_test');
    }
}
