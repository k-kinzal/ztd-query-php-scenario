<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPSERT and REPLACE with prepared statements on MySQLi.
 * @spec SPEC-4.2a
 */
class PreparedUpsertTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_prep_upsert (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_prep_upsert'];
    }


    public function testPreparedUpsertInserts(): void
    {
        $stmt = $this->mysqli->prepare(
            'INSERT INTO mi_prep_upsert (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)'
        );
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name, score FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Prepared UPSERT correctly updates existing rows on duplicate key.
     */
    public function testPreparedUpsertUpdatesExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->mysqli->prepare(
            'INSERT INTO mi_prep_upsert (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)'
        );
        $id = 1;
        $name = 'Updated';
        $score = 200;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name, score FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Updated', $row['name']);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * Prepared REPLACE correctly replaces existing rows.
     */
    public function testPreparedReplaceReplacesExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->mysqli->prepare('REPLACE INTO mi_prep_upsert (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Replaced';
        $score = 999;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name, score FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Replaced', $row['name']);
        $this->assertSame(999, (int) $row['score']);
    }

    /**
     * Non-prepared UPSERT via query() works correctly.
     */
    public function testQueryUpsertWorksCorrectly(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Original', 50)");
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Updated', 200) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)");

        $result = $this->mysqli->query('SELECT name FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Updated', $row['name']);
    }
}
