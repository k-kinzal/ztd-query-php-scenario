<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite INSERT OR REPLACE / INSERT OR IGNORE through CTE shadow.
 *
 * SQLite supports conflict resolution clauses:
 *   INSERT OR REPLACE INTO t ... (equivalent to REPLACE INTO on MySQL)
 *   INSERT OR IGNORE INTO t ...
 *
 * Known Issue: INSERT OR REPLACE via prepared statement does not delete the
 * existing row in the shadow store, creating duplicate primary keys. The
 * exec() path works correctly. This mirrors the MySQL REPLACE prepared
 * statement bug.
 *
 * @spec SPEC-4.4
 * @see SPEC-11.PDO-REPLACE-PREPARED
 */
class SqliteInsertOrReplaceTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sq_ior (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sq_ior'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sq_ior VALUES (1, 'original', 100)");
        $this->pdo->exec("INSERT INTO sq_ior VALUES (2, 'keeper', 200)");
    }

    /**
     * INSERT OR REPLACE - new row (no conflict).
     */
    public function testInsertOrReplaceNewRow(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO sq_ior VALUES (3, 'new', 300)");

        $rows = $this->ztdQuery('SELECT * FROM sq_ior ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('new', $rows[2]['name']);
    }

    /**
     * INSERT OR REPLACE via exec() - existing PK (works correctly).
     */
    public function testInsertOrReplaceExecExistingPk(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO sq_ior VALUES (1, 'replaced', 999)");

        $rows = $this->ztdQuery('SELECT name, value FROM sq_ior WHERE id = 1');
        $this->assertCount(1, $rows, 'INSERT OR REPLACE via exec should replace correctly');
        $this->assertSame('replaced', $rows[0]['name']);
        $this->assertEquals(999, (int) $rows[0]['value']);
    }

    /**
     * INSERT OR REPLACE via exec() - row count unchanged on conflict.
     */
    public function testInsertOrReplaceExecRowCount(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO sq_ior VALUES (1, 'replaced', 999)");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sq_ior');
        $this->assertEquals(2, (int) $rows[0]['cnt'], 'Row count should remain 2');
    }

    /**
     * INSERT OR REPLACE with prepared statement creates duplicate PK (Known Issue).
     *
     * Expected: 1 row with id=1, name='prep-replaced'
     * Actual: 2 rows with id=1 (original + new)
     */
    public function testInsertOrReplacePreparedCreatesDuplicate(): void
    {
        $stmt = $this->pdo->prepare('INSERT OR REPLACE INTO sq_ior VALUES (?, ?, ?)');
        $stmt->execute([1, 'prep-replaced', 888]);

        $rows = $this->ztdQuery('SELECT name FROM sq_ior WHERE id = 1');
        // Known Issue: Creates duplicate instead of replacing
        $this->assertCount(2, $rows, 'Known Issue: Prepared INSERT OR REPLACE creates duplicate PK');
    }

    /**
     * INSERT OR IGNORE - skip on conflict (works correctly).
     */
    public function testInsertOrIgnoreConflict(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO sq_ior VALUES (1, 'ignored', 0)");

        $rows = $this->ztdQuery('SELECT name, value FROM sq_ior WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('original', $rows[0]['name'], 'INSERT OR IGNORE should keep original');
        $this->assertEquals(100, (int) $rows[0]['value']);
    }

    /**
     * INSERT OR IGNORE - new row (no conflict).
     */
    public function testInsertOrIgnoreNewRow(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO sq_ior VALUES (3, 'new', 300)");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sq_ior');
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * INSERT OR IGNORE with prepared statement on conflict.
     */
    public function testInsertOrIgnorePreparedConflict(): void
    {
        $stmt = $this->pdo->prepare('INSERT OR IGNORE INTO sq_ior VALUES (?, ?, ?)');
        $stmt->execute([1, 'ignored', 0]);

        $rows = $this->ztdQuery('SELECT name FROM sq_ior WHERE id = 1');
        $this->assertSame('original', $rows[0]['name'], 'Prepared INSERT OR IGNORE should keep original');
    }

    /**
     * INSERT OR REPLACE multiple times via exec (works correctly).
     */
    public function testInsertOrReplaceExecMultipleTimes(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO sq_ior VALUES (1, 'v1', 1)");
        $this->pdo->exec("INSERT OR REPLACE INTO sq_ior VALUES (1, 'v2', 2)");
        $this->pdo->exec("INSERT OR REPLACE INTO sq_ior VALUES (1, 'v3', 3)");

        $rows = $this->ztdQuery('SELECT name, value FROM sq_ior WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('v3', $rows[0]['name'], 'Last REPLACE should win');
        $this->assertEquals(3, (int) $rows[0]['value']);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sq_ior');
        $this->assertEquals(2, (int) $rows[0]['cnt'], 'Total row count should remain 2');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_ior');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
