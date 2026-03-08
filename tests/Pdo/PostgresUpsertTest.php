<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/** @spec SPEC-4.2a */
class PostgresUpsertTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE upsert_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['upsert_test'];
    }


    public function testInsertOnConflictDoUpdateInserts(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testInsertOnConflictDoUpdateUpdates(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('updated', $rows[0]['val']);
    }

    public function testInsertOnConflictDoNothingIgnoresDuplicate(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'ignored') ON CONFLICT (id) DO NOTHING");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('original', $rows[0]['val']);
    }

    public function testUpsertIsolation(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM upsert_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
