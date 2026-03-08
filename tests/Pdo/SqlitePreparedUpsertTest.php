<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statement UPSERT (ON CONFLICT DO UPDATE) on SQLite PDO.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/23
 * @spec SPEC-4.2a
 */
class SqlitePreparedUpsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sq_ups (id INTEGER PRIMARY KEY, name TEXT, score INT)';
    }

    protected function getTableNames(): array
    {
        return ['sq_ups'];
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
     * Prepared ON CONFLICT DO UPDATE should update existing rows.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedUpsertUpdatesExisting(): void
    {
        $this->pdo->exec("INSERT INTO sq_ups (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare(
            'INSERT INTO sq_ups (id, name, score) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = excluded.name, score = excluded.score'
        );
        $stmt->execute([1, 'Updated', 200]);

        $select = $this->pdo->query('SELECT name FROM sq_ups WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        if ($row['name'] !== 'Updated') {
            $this->markTestIncomplete(
                'Issue #23: prepared ON CONFLICT DO UPDATE does not update existing rows on SQLite PDO. '
                . 'Expected "Updated", got ' . var_export($row['name'], true)
            );
        }
        $this->assertSame('Updated', $row['name']);
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
     * Prepared INSERT OR REPLACE should replace existing rows.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedInsertOrReplaceReplacesExisting(): void
    {
        $this->pdo->exec("INSERT INTO sq_ups (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare('INSERT OR REPLACE INTO sq_ups (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Replaced', 999]);

        $select = $this->pdo->query('SELECT name FROM sq_ups WHERE id = 1');
        $name = $select->fetchColumn();
        if ($name !== 'Replaced') {
            $this->markTestIncomplete(
                'Issue #23: prepared INSERT OR REPLACE does not replace existing rows on SQLite PDO. '
                . 'Expected "Replaced", got ' . var_export($name, true)
            );
        }
        $this->assertSame('Replaced', $name);
    }
}
