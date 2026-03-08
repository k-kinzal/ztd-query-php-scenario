<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests sequential mutations on the same table on PostgreSQL PDO to verify
 * shadow store correctly accumulates changes across multiple operations.
 * @spec pending
 */
class PostgresSequentialMutationsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_seq_test (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_seq_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (1, 'Alice', 'active', 90)");
        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (2, 'Bob', 'active', 80)");
        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (3, 'Charlie', 'active', 70)");
    }

    /**
     * Insert then update the same row.
     */
    public function testInsertThenUpdate(): void
    {
        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (4, 'Dave', 'new', 60)");
        $this->pdo->exec("UPDATE pg_seq_test SET status = 'active' WHERE id = 4");

        $stmt = $this->pdo->query("SELECT status FROM pg_seq_test WHERE id = 4");
        $this->assertSame('active', $stmt->fetchColumn());
    }

    /**
     * Insert then delete the same row.
     */
    public function testInsertThenDelete(): void
    {
        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (4, 'Dave', 'temp', 0)");
        $this->pdo->exec("DELETE FROM pg_seq_test WHERE id = 4");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pg_seq_test WHERE id = 4");
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_seq_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Update then delete the same row.
     */
    public function testUpdateThenDelete(): void
    {
        $this->pdo->exec("UPDATE pg_seq_test SET score = 100 WHERE id = 1");
        $this->pdo->exec("DELETE FROM pg_seq_test WHERE id = 1");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pg_seq_test WHERE id = 1");
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_seq_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Multiple updates on the same row — last write wins.
     */
    public function testMultipleUpdatesOnSameRow(): void
    {
        $this->pdo->exec("UPDATE pg_seq_test SET score = 91 WHERE id = 1");
        $this->pdo->exec("UPDATE pg_seq_test SET score = 92 WHERE id = 1");
        $this->pdo->exec("UPDATE pg_seq_test SET score = 93 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT score FROM pg_seq_test WHERE id = 1");
        $this->assertSame(93, (int) $stmt->fetchColumn());
    }

    /**
     * Delete all then insert new data.
     */
    public function testDeleteAllThenInsert(): void
    {
        $this->pdo->exec("DELETE FROM pg_seq_test WHERE 1=1");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_seq_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (10, 'New', 'fresh', 100)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_seq_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Interleaved inserts and deletes.
     */
    public function testInterleavedInsertsAndDeletes(): void
    {
        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (4, 'Dave', 'active', 60)");
        $this->pdo->exec("DELETE FROM pg_seq_test WHERE id = 2");
        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (5, 'Eve', 'active', 50)");
        $this->pdo->exec("DELETE FROM pg_seq_test WHERE id = 3");

        $stmt = $this->pdo->query('SELECT name FROM pg_seq_test ORDER BY id');
        $names = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Dave', 'Eve'], $names);
    }

    /**
     * Bulk update then selective delete.
     */
    public function testBulkUpdateThenSelectiveDelete(): void
    {
        $this->pdo->exec("UPDATE pg_seq_test SET status = 'archived' WHERE score < 80");
        $this->pdo->exec("DELETE FROM pg_seq_test WHERE status = 'archived'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_seq_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM pg_seq_test ORDER BY id');
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
        $this->pdo->exec("INSERT INTO pg_seq_test (id, name, status, score) VALUES (4, 'Dave', 'active', 60)");
        $this->pdo->exec("DELETE FROM pg_seq_test WHERE id = 3");

        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_seq_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
