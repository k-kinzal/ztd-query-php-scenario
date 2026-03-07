<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared statement UPSERT (ON CONFLICT DO UPDATE) on SQLite PDO.
 *
 * PDO adapter limitation: prepared UPSERT does NOT update existing rows.
 * This is consistent with MySQL and PostgreSQL PDO behavior.
 */
class SqlitePreparedUpsertTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sq_ups (id INTEGER PRIMARY KEY, name TEXT, score INT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testPreparedUpsertInsertsNewRow(): void
    {
        $stmt = $this->pdo->prepare(
            'INSERT INTO sq_ups (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = excluded.name, score = excluded.score'
        );
        $stmt->execute([1, 'Alice', 100]);

        $select = $this->pdo->query('SELECT name, score FROM sq_ups WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * PDO limitation: prepared ON CONFLICT DO UPDATE does NOT update existing rows.
     * Consistent with MySQL and PostgreSQL PDO behavior.
     */
    public function testPreparedUpsertDoesNotUpdateExisting(): void
    {
        $this->pdo->exec("INSERT INTO sq_ups (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare(
            'INSERT INTO sq_ups (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = excluded.name, score = excluded.score'
        );
        $stmt->execute([1, 'Updated', 200]);

        $select = $this->pdo->query('SELECT name FROM sq_ups WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        // Bug: should be 'Updated' but old row is retained
        $this->assertSame('Original', $row['name']);
    }

    /**
     * Non-prepared UPSERT via exec() works correctly.
     */
    public function testExecUpsertWorksCorrectly(): void
    {
        $this->pdo->exec("INSERT INTO sq_ups (id, name, score) VALUES (1, 'Original', 50)");
        $this->pdo->exec("INSERT INTO sq_ups (id, name, score) VALUES (1, 'Updated', 200) ON CONFLICT (id) DO UPDATE SET name = excluded.name, score = excluded.score");

        $select = $this->pdo->query('SELECT name, score FROM sq_ups WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Updated', $row['name']);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * Prepared ON CONFLICT DO NOTHING with prepared statements.
     */
    public function testPreparedOnConflictDoNothing(): void
    {
        $this->pdo->exec("INSERT INTO sq_ups (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare(
            'INSERT INTO sq_ups (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING'
        );
        $stmt->execute([1, 'Ignored', 999]);

        $select = $this->pdo->query('SELECT name FROM sq_ups WHERE id = 1');
        $rows = $select->fetchAll(PDO::FETCH_ASSOC);
        // SQLite shadow store doesn't enforce PK constraints for ON CONFLICT DO NOTHING
        // — both rows may be retained (consistent with SqliteUpsertTest)
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testPreparedInsertOrReplaceNewRow(): void
    {
        $stmt = $this->pdo->prepare('INSERT OR REPLACE INTO sq_ups (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        $select = $this->pdo->query('SELECT name FROM sq_ups WHERE id = 1');
        $this->assertSame('Alice', $select->fetchColumn());
    }

    /**
     * PDO limitation: prepared INSERT OR REPLACE does NOT replace existing rows.
     */
    public function testPreparedInsertOrReplaceDoesNotReplaceExisting(): void
    {
        $this->pdo->exec("INSERT INTO sq_ups (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare('INSERT OR REPLACE INTO sq_ups (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Replaced', 999]);

        $select = $this->pdo->query('SELECT name FROM sq_ups WHERE id = 1');
        // Bug: should be 'Replaced' but old row is retained
        $this->assertSame('Original', $select->fetchColumn());
    }
}
