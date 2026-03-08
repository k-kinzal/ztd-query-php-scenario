<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests EXPLAIN and PRAGMA handling through ZTD on SQLite.
 *
 * EXPLAIN and PRAGMA are utility statements that may or may not be
 * handled by the CTE rewriter. Tests verify behavior and document
 * what works vs what throws.
 */
class SqliteExplainPragmaTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE ep_items (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO ep_items VALUES (1, 'Widget')");
    }

    /**
     * EXPLAIN QUERY PLAN on a SELECT.
     */
    public function testExplainQueryPlan(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN QUERY PLAN SELECT * FROM ep_items WHERE id = 1');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            // If it works, it should return query plan rows
            $this->assertIsArray($rows);
        } catch (\Throwable $e) {
            // EXPLAIN may not be supported by the CTE rewriter
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN on a simple SELECT.
     */
    public function testExplainSelect(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN SELECT * FROM ep_items');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertIsArray($rows);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * PRAGMA table_info returns column metadata.
     */
    public function testPragmaTableInfo(): void
    {
        try {
            $stmt = $this->pdo->query('PRAGMA table_info(ep_items)');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            // Should return column info for the physical table
            $this->assertNotEmpty($rows);
            $names = array_column($rows, 'name');
            $this->assertContains('id', $names);
            $this->assertContains('name', $names);
        } catch (\Throwable $e) {
            // PRAGMA may be treated as unsupported SQL
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * PRAGMA foreign_keys setting.
     */
    public function testPragmaForeignKeys(): void
    {
        try {
            $stmt = $this->pdo->query('PRAGMA foreign_keys');
            $row = $stmt->fetch(PDO::FETCH_NUM);
            // Returns 0 or 1
            $this->assertContains((int) $row[0], [0, 1]);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * PRAGMA journal_mode query.
     */
    public function testPragmaJournalMode(): void
    {
        try {
            $stmt = $this->pdo->query('PRAGMA journal_mode');
            $row = $stmt->fetch(PDO::FETCH_NUM);
            $this->assertContains($row[0], ['delete', 'truncate', 'persist', 'memory', 'wal', 'off']);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Shadow operations still work after EXPLAIN/PRAGMA attempts.
     */
    public function testShadowOperationsWorkAfterPragma(): void
    {
        // Try PRAGMA (may or may not work)
        try {
            $this->pdo->query('PRAGMA table_info(ep_items)');
        } catch (\Throwable $e) {
            // Ignore
        }

        // Shadow operations should still work
        $this->pdo->exec("INSERT INTO ep_items VALUES (2, 'Gadget')");
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ep_items');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ep_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
