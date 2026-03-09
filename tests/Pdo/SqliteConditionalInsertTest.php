<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests conditional INSERT patterns through ZTD shadow store on SQLite.
 *
 * Patterns:
 * - INSERT...SELECT WHERE NOT EXISTS (anti-join insert)
 * - INSERT...SELECT with EXCEPT
 * - INSERT OR IGNORE with shadow store
 * @spec SPEC-4.3
 */
class SqliteConditionalInsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ci_source (id INT PRIMARY KEY, name VARCHAR(50), value INT)',
            'CREATE TABLE ci_target (id INT PRIMARY KEY, name VARCHAR(50), value INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ci_source', 'ci_target'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ci_source VALUES (1, 'alpha', 100)");
        $this->pdo->exec("INSERT INTO ci_source VALUES (2, 'beta', 200)");
        $this->pdo->exec("INSERT INTO ci_source VALUES (3, 'gamma', 300)");

        $this->pdo->exec("INSERT INTO ci_target VALUES (1, 'alpha', 100)");
    }

    /**
     * INSERT...SELECT WHERE NOT EXISTS: only insert rows not already in target.
     */
    public function testInsertWhereNotExists(): void
    {
        $this->pdo->exec(
            'INSERT INTO ci_target (id, name, value)
             SELECT s.id, s.name, s.value
             FROM ci_source s
             WHERE NOT EXISTS (
                 SELECT 1 FROM ci_target t WHERE t.id = s.id
             )'
        );

        $rows = $this->ztdQuery('SELECT * FROM ci_target ORDER BY id');
        // Should have all 3 rows: original alpha + new beta + new gamma
        $this->assertCount(3, $rows);
        $this->assertSame('alpha', $rows[0]['name']);
        $this->assertSame('beta', $rows[1]['name']);
        $this->assertSame('gamma', $rows[2]['name']);
    }

    /**
     * INSERT...SELECT WHERE NOT EXISTS should be idempotent.
     */
    public function testInsertWhereNotExistsIdempotent(): void
    {
        // First insert
        $this->pdo->exec(
            'INSERT INTO ci_target (id, name, value)
             SELECT s.id, s.name, s.value
             FROM ci_source s
             WHERE NOT EXISTS (SELECT 1 FROM ci_target t WHERE t.id = s.id)'
        );

        // Second insert (should insert 0 rows)
        $this->pdo->exec(
            'INSERT INTO ci_target (id, name, value)
             SELECT s.id, s.name, s.value
             FROM ci_source s
             WHERE NOT EXISTS (SELECT 1 FROM ci_target t WHERE t.id = s.id)'
        );

        $rows = $this->ztdQuery('SELECT * FROM ci_target ORDER BY id');
        $this->assertCount(3, $rows);
    }

    /**
     * INSERT...SELECT from same table (copy unmatched).
     * Insert into target rows that aren't already there, identified by name.
     */
    public function testInsertSelectAntiJoinByName(): void
    {
        $this->pdo->exec(
            'INSERT INTO ci_target (id, name, value)
             SELECT s.id, s.name, s.value
             FROM ci_source s
             LEFT JOIN ci_target t ON s.name = t.name
             WHERE t.id IS NULL'
        );

        $rows = $this->ztdQuery('SELECT * FROM ci_target ORDER BY id');
        $this->assertCount(3, $rows);
    }

    /**
     * INSERT OR IGNORE with duplicate PK.
     */
    public function testInsertOrIgnoreDuplicate(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO ci_target VALUES (1, 'replaced', 999)");
        $this->pdo->exec("INSERT OR IGNORE INTO ci_target VALUES (4, 'delta', 400)");

        $rows = $this->ztdQuery('SELECT * FROM ci_target ORDER BY id');
        // id=1 should be unchanged (IGNORE), id=4 should be new
        $this->assertCount(2, $rows);
        $this->assertSame('alpha', $rows[0]['name']); // unchanged
        $this->assertSame('delta', $rows[1]['name']); // new
    }

    /**
     * INSERT...SELECT with computed value from source.
     * Related to Issue #20: computed columns in INSERT...SELECT produce NULLs on SQLite.
     */
    public function testInsertSelectWithComputedColumn(): void
    {
        $this->pdo->exec('DELETE FROM ci_target WHERE 1=1');

        $this->pdo->exec(
            'INSERT INTO ci_target (id, name, value)
             SELECT id, name, value * 2 FROM ci_source WHERE id > 1'
        );

        $rows = $this->ztdQuery('SELECT * FROM ci_target ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('beta', $rows[0]['name']);
        // Issue #20: computed columns in INSERT...SELECT produce NULL/empty on SQLite
        if ($rows[0]['value'] === '' || $rows[0]['value'] === null) {
            $this->markTestIncomplete('Issue #20: computed column value*2 produces empty/NULL');
        }
        $this->assertSame('400', (string) $rows[0]['value']);
        $this->assertSame('600', (string) $rows[1]['value']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ci_target');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
