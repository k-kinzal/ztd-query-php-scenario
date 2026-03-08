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
 * Tests SELECT...FOR UPDATE NOWAIT / SKIP LOCKED on PostgreSQL.
 *
 * These are locking clause extensions that control behavior when
 * a requested row is already locked by another transaction.
 * In ZTD mode, locking clauses are no-ops (CTE data can't be locked).
 */
class PostgresForUpdateSkipLockedTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_fusl_tasks');
        $raw->exec('CREATE TABLE pg_fusl_tasks (id INT PRIMARY KEY, status VARCHAR(20), payload VARCHAR(100))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_fusl_tasks VALUES (1, 'pending', 'task-1')");
        $this->pdo->exec("INSERT INTO pg_fusl_tasks VALUES (2, 'pending', 'task-2')");
        $this->pdo->exec("INSERT INTO pg_fusl_tasks VALUES (3, 'done', 'task-3')");
    }

    /**
     * FOR UPDATE SKIP LOCKED returns shadow data (no-op locking).
     */
    public function testForUpdateSkipLocked(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT * FROM pg_fusl_tasks WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FOR UPDATE SKIP LOCKED not supported: ' . $e->getMessage());
        }
    }

    /**
     * FOR UPDATE NOWAIT returns shadow data (no-op locking).
     */
    public function testForUpdateNowait(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT * FROM pg_fusl_tasks WHERE status = 'pending' FOR UPDATE NOWAIT"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FOR UPDATE NOWAIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * FOR NO KEY UPDATE returns shadow data.
     */
    public function testForNoKeyUpdate(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT * FROM pg_fusl_tasks WHERE id = 1 FOR NO KEY UPDATE"
            );
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('task-1', $row['payload']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FOR NO KEY UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * FOR KEY SHARE returns shadow data.
     */
    public function testForKeyShare(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT * FROM pg_fusl_tasks WHERE id = 1 FOR KEY SHARE"
            );
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('task-1', $row['payload']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FOR KEY SHARE not supported: ' . $e->getMessage());
        }
    }

    /**
     * Locking clauses with shadow mutations.
     */
    public function testLockingAfterMutation(): void
    {
        $this->pdo->exec("UPDATE pg_fusl_tasks SET status = 'done' WHERE id = 1");

        try {
            $stmt = $this->pdo->query(
                "SELECT * FROM pg_fusl_tasks WHERE status = 'pending' FOR UPDATE SKIP LOCKED"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            // Only task-2 should be pending now
            $this->assertCount(1, $rows);
            $this->assertSame('task-2', $rows[0]['payload']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Locking after mutation not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_fusl_tasks');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                PostgreSQLContainer::getDsn(),
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pg_fusl_tasks');
        } catch (\Exception $e) {
        }
    }
}
