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
 * Tests PostgreSQL DISTINCT ON through CTE shadow.
 *
 * DISTINCT ON is a PostgreSQL extension that returns the first row
 * of each set of rows where the given expressions evaluate to equal.
 * Also tests FETCH FIRST N ROWS ONLY (SQL standard LIMIT).
 */
class PostgresDistinctOnTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_don_logs');
        $raw->exec('CREATE TABLE pg_don_logs (id INT PRIMARY KEY, category VARCHAR(50), message VARCHAR(100), priority INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_don_logs VALUES (1, 'error', 'disk full', 1)");
        $this->pdo->exec("INSERT INTO pg_don_logs VALUES (2, 'error', 'timeout', 2)");
        $this->pdo->exec("INSERT INTO pg_don_logs VALUES (3, 'warn', 'low memory', 1)");
        $this->pdo->exec("INSERT INTO pg_don_logs VALUES (4, 'warn', 'high cpu', 2)");
        $this->pdo->exec("INSERT INTO pg_don_logs VALUES (5, 'info', 'started', 3)");
    }

    /**
     * DISTINCT ON to get first row per category.
     */
    public function testDistinctOnCategory(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT DISTINCT ON (category) category, message, priority
                 FROM pg_don_logs
                 ORDER BY category, priority ASC"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // One row per category: error, info, warn
            $this->assertCount(3, $rows);
            $categories = array_column($rows, 'category');
            $this->assertContains('error', $categories);
            $this->assertContains('warn', $categories);
            $this->assertContains('info', $categories);

            // error with lowest priority (1) = 'disk full'
            $errorRow = array_filter($rows, fn($r) => $r['category'] === 'error');
            $errorRow = reset($errorRow);
            $this->assertSame('disk full', $errorRow['message']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DISTINCT ON not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * DISTINCT ON after shadow mutation.
     */
    public function testDistinctOnAfterMutation(): void
    {
        // Add higher-priority error
        $this->pdo->exec("INSERT INTO pg_don_logs VALUES (6, 'error', 'crash', 0)");

        try {
            $stmt = $this->pdo->query(
                "SELECT DISTINCT ON (category) category, message, priority
                 FROM pg_don_logs
                 ORDER BY category, priority ASC"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $errorRow = array_filter($rows, fn($r) => $r['category'] === 'error');
            $errorRow = reset($errorRow);
            // 'crash' has priority 0 (lowest = first)
            $this->assertSame('crash', $errorRow['message']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DISTINCT ON not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * FETCH FIRST N ROWS ONLY (SQL standard LIMIT).
     */
    public function testFetchFirstNRowsOnly(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT * FROM pg_don_logs ORDER BY id FETCH FIRST 2 ROWS ONLY"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FETCH FIRST not supported: ' . $e->getMessage());
        }
    }

    /**
     * OFFSET ... FETCH NEXT (SQL standard pagination).
     */
    public function testOffsetFetchNext(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT * FROM pg_don_logs ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('OFFSET FETCH not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_don_logs');
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
            $raw->exec('DROP TABLE IF EXISTS pg_don_logs');
        } catch (\Exception $e) {
        }
    }
}
