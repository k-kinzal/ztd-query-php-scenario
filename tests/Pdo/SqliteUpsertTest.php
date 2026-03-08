<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/** @spec SPEC-4.2a */
class SqliteUpsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE upsert_test (id INTEGER PRIMARY KEY, val TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['upsert_test'];
    }

    private PDO $raw;

    protected function setUp(): void
    {
        parent::setUp();

        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE upsert_test (id INTEGER PRIMARY KEY, val TEXT)');

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

    public function testInsertOnConflictDoNothingInsertsDuplicate(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'not_ignored') ON CONFLICT (id) DO NOTHING");

        // On SQLite, ON CONFLICT DO NOTHING does not prevent the insert
        // in the shadow store — both rows are kept (shadow store doesn't
        // enforce PK constraints, see section 8.1)
        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testReplaceIntoInserts(): void
    {
        $this->pdo->exec("REPLACE INTO upsert_test (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testReplaceIntoReplaces(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("REPLACE INTO upsert_test (id, val) VALUES (1, 'replaced')");

        $stmt = $this->pdo->query('SELECT * FROM upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('replaced', $rows[0]['val']);
    }

    public function testUpsertIsolation(): void
    {
        $this->pdo->exec("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM upsert_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
