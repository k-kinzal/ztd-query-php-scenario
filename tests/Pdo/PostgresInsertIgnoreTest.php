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
 * Tests INSERT ... ON CONFLICT DO NOTHING behavior on PostgreSQL ZTD:
 * - Duplicate PK silently skipped
 * - Non-duplicate rows inserted
 * - Batch with mixed duplicates
 * - Prepared ON CONFLICT DO NOTHING
 * - Physical isolation
 */
class PostgresInsertIgnoreTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_ins_ign');
        $raw->exec('CREATE TABLE pg_ins_ign (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_ins_ign VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_ins_ign VALUES (2, 'Bob', 80)");
    }

    public function testOnConflictDoNothingDuplicateSkipped(): void
    {
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (1, 'AliceV2', 99) ON CONFLICT (id) DO NOTHING");

        $stmt = $this->pdo->query('SELECT name, score FROM pg_ins_ign WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(90, $row['score']);
    }

    public function testOnConflictDoNothingNonDuplicateInserted(): void
    {
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (3, 'Charlie', 70) ON CONFLICT (id) DO NOTHING");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ins_ign');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM pg_ins_ign WHERE id = 3');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    public function testOnConflictDoNothingAllDuplicates(): void
    {
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (1, 'DupAlice', 99) ON CONFLICT (id) DO NOTHING");
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (2, 'DupBob', 99) ON CONFLICT (id) DO NOTHING");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ins_ign');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testPreparedOnConflictDoNothing(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_ins_ign (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING');

        // Duplicate PK
        $stmt->execute([1, 'DupAlice', 99]);

        $check = $this->pdo->query('SELECT name FROM pg_ins_ign WHERE id = 1');
        $this->assertSame('Alice', $check->fetchColumn());

        // Non-duplicate PK
        $stmt->execute([3, 'Charlie', 70]);

        $check = $this->pdo->query('SELECT name FROM pg_ins_ign WHERE id = 3');
        $this->assertSame('Charlie', $check->fetchColumn());
    }

    public function testOnConflictDoNothingDoesNotAffectSubsequentInserts(): void
    {
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (1, 'DupAlice', 99) ON CONFLICT (id) DO NOTHING");

        // Normal INSERT with new PK after ON CONFLICT DO NOTHING still works
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (4, 'Diana', 85)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ins_ign');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testOnConflictDoNothingPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (1, 'DupAlice', 99) ON CONFLICT (id) DO NOTHING");
        $this->pdo->exec("INSERT INTO pg_ins_ign (id, name, score) VALUES (3, 'Charlie', 70) ON CONFLICT (id) DO NOTHING");

        // Physical table should be empty (no shadow data leaked)
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ins_ign');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_ins_ign');
    }
}
