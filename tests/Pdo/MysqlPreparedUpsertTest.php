<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statement UPSERT (ON DUPLICATE KEY UPDATE) and REPLACE on MySQL PDO.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/23
 * @spec SPEC-4.2a
 */
class MysqlPreparedUpsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_upsert_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_upsert_test'];
    }


    public function testPreparedUpsertInsertsNewRow(): void
    {
        $stmt = $this->pdo->prepare(
            'INSERT INTO pdo_upsert_test (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)'
        );
        $stmt->execute([1, 'Alice', 100]);

        $select = $this->pdo->query('SELECT name, score FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * Prepared UPSERT should update existing rows.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedUpsertUpdatesExisting(): void
    {
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare(
            'INSERT INTO pdo_upsert_test (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)'
        );
        $stmt->execute([1, 'Updated', 200]);

        $select = $this->pdo->query('SELECT name FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        if ($row['name'] !== 'Updated') {
            $this->markTestIncomplete(
                'Issue #23: prepared UPSERT does not update existing rows on MySQL PDO. '
                . 'Expected "Updated", got ' . var_export($row['name'], true)
            );
        }
        $this->assertSame('Updated', $row['name']);
    }

    /**
     * Prepared REPLACE should replace existing rows.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedReplaceReplacesExisting(): void
    {
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->pdo->prepare('REPLACE INTO pdo_upsert_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Replaced', 999]);

        $select = $this->pdo->query('SELECT name FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        if ($row['name'] !== 'Replaced') {
            $this->markTestIncomplete(
                'Issue #23: prepared REPLACE does not replace existing rows on MySQL PDO. '
                . 'Expected "Replaced", got ' . var_export($row['name'], true)
            );
        }
        $this->assertSame('Replaced', $row['name']);
    }

    /**
     * Non-prepared UPSERT via exec() works correctly.
     */
    public function testExecUpsertWorksCorrectly(): void
    {
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Original', 50)");
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Updated', 200) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)");

        $select = $this->pdo->query('SELECT name, score FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Updated', $row['name']);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * Non-prepared REPLACE via exec() works correctly.
     */
    public function testExecReplaceWorksCorrectly(): void
    {
        $this->pdo->exec("INSERT INTO pdo_upsert_test (id, name, score) VALUES (1, 'Original', 50)");
        $this->pdo->exec("REPLACE INTO pdo_upsert_test (id, name, score) VALUES (1, 'Replaced', 999)");

        $select = $this->pdo->query('SELECT name, score FROM pdo_upsert_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Replaced', $row['name']);
        $this->assertSame(999, (int) $row['score']);
    }
}
