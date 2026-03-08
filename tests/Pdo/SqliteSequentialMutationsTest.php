<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests sequential mutations on the same table to verify shadow store
 * correctly accumulates changes across multiple operations.
 *
 * All seed data is inserted via ZTD mode (not physical) because once
 * schema reflection occurs, the CTE replaces physical table references.
 * Physical rows are NOT visible through ZTD SELECT.
 * @spec SPEC-4.8
 */
class SqliteSequentialMutationsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE seq_test (id INTEGER PRIMARY KEY, name TEXT, status TEXT, score INT)';
    }

    protected function getTableNames(): array
    {
        return ['seq_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (1, 'Alice', 'active', 90)");
        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (2, 'Bob', 'active', 80)");
        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (3, 'Charlie', 'active', 70)");
    }
    /**
     * Insert then update the same row.
     */
    public function testInsertThenUpdate(): void
    {
        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (4, 'Dave', 'new', 60)");
        $this->pdo->exec("UPDATE seq_test SET status = 'active' WHERE id = 4");

        $stmt = $this->pdo->query("SELECT status FROM seq_test WHERE id = 4");
        $this->assertSame('active', $stmt->fetchColumn());
    }

    /**
     * Insert then delete the same row.
     */
    public function testInsertThenDelete(): void
    {
        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (4, 'Dave', 'temp', 0)");
        $this->pdo->exec("DELETE FROM seq_test WHERE id = 4");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM seq_test WHERE id = 4");
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // Original ZTD-inserted rows still present
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM seq_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Update then delete the same row.
     */
    public function testUpdateThenDelete(): void
    {
        $this->pdo->exec("UPDATE seq_test SET score = 100 WHERE id = 1");
        $this->pdo->exec("DELETE FROM seq_test WHERE id = 1");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM seq_test WHERE id = 1");
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM seq_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Multiple updates on the same row.
     */
    public function testMultipleUpdatesOnSameRow(): void
    {
        $this->pdo->exec("UPDATE seq_test SET score = 91 WHERE id = 1");
        $this->pdo->exec("UPDATE seq_test SET score = 92 WHERE id = 1");
        $this->pdo->exec("UPDATE seq_test SET score = 93 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT score FROM seq_test WHERE id = 1");
        $this->assertSame(93, (int) $stmt->fetchColumn());
    }

    /**
     * Delete all then insert new data.
     */
    public function testDeleteAllThenInsert(): void
    {
        $this->pdo->exec("DELETE FROM seq_test WHERE 1=1");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM seq_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (10, 'New', 'fresh', 100)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM seq_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Interleaved inserts and deletes.
     */
    public function testInterleavedInsertsAndDeletes(): void
    {
        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (4, 'Dave', 'active', 60)");
        $this->pdo->exec("DELETE FROM seq_test WHERE id = 2"); // Remove Bob
        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (5, 'Eve', 'active', 50)");
        $this->pdo->exec("DELETE FROM seq_test WHERE id = 3"); // Remove Charlie

        $stmt = $this->pdo->query('SELECT name FROM seq_test ORDER BY id');
        $names = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Dave', 'Eve'], $names);
    }

    /**
     * Update multiple rows then delete some.
     */
    public function testBulkUpdateThenSelectiveDelete(): void
    {
        $this->pdo->exec("UPDATE seq_test SET status = 'archived' WHERE score < 80");
        $this->pdo->exec("DELETE FROM seq_test WHERE status = 'archived'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM seq_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM seq_test ORDER BY id');
        $names = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    /**
     * Physical isolation after complex mutations.
     */
    public function testPhysicalIsolationAfterComplexMutations(): void
    {
        $this->pdo->exec("INSERT INTO seq_test (id, name, status, score) VALUES (4, 'Dave', 'active', 60)");
        $this->pdo->exec("DELETE FROM seq_test WHERE id = 3");

        $this->pdo->disableZtd();

        // Physical DB has no rows (data was only in shadow)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM seq_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
