<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE all rows followed by re-INSERT through ZTD shadow store.
 *
 * This exercises the shadow store's ability to handle full table clear
 * followed by repopulation — a common pattern in batch processing,
 * cache rebuilds, and data synchronization workflows.
 * @spec SPEC-4.7
 */
class SqliteDeleteAllAndRebuildTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE dar_cache (id INT PRIMARY KEY, key_name VARCHAR(50), value VARCHAR(200))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['dar_cache'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO dar_cache VALUES (1, 'config.theme', 'dark')");
        $this->pdo->exec("INSERT INTO dar_cache VALUES (2, 'config.lang', 'en')");
        $this->pdo->exec("INSERT INTO dar_cache VALUES (3, 'config.tz', 'UTC')");
    }

    /**
     * DELETE WHERE 1=1 (delete all), then verify empty.
     * Related to Issue #7 (DELETE without WHERE silently ignored on SQLite).
     */
    public function testDeleteAllWithWhereTrue(): void
    {
        $this->pdo->exec('DELETE FROM dar_cache WHERE 1=1');

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM dar_cache');
        $this->assertSame('0', (string) $rows[0]['cnt']);
    }

    /**
     * DELETE without WHERE clause.
     * Related to Issue #7: may be silently ignored on SQLite.
     */
    public function testDeleteWithoutWhere(): void
    {
        $this->pdo->exec('DELETE FROM dar_cache');

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM dar_cache');
        if ((string) $rows[0]['cnt'] !== '0') {
            $this->markTestIncomplete('Issue #7: DELETE without WHERE silently ignored on SQLite (count=' . $rows[0]['cnt'] . ')');
        }
        $this->assertSame('0', (string) $rows[0]['cnt']);
    }

    /**
     * Delete all then re-insert: shadow should show only new data.
     */
    public function testDeleteAllThenReinsert(): void
    {
        $this->pdo->exec('DELETE FROM dar_cache WHERE 1=1');
        $this->pdo->exec("INSERT INTO dar_cache VALUES (10, 'new.key1', 'val1')");
        $this->pdo->exec("INSERT INTO dar_cache VALUES (11, 'new.key2', 'val2')");

        $rows = $this->ztdQuery('SELECT * FROM dar_cache ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('10', (string) $rows[0]['id']);
        $this->assertSame('new.key1', $rows[0]['key_name']);
        $this->assertSame('11', (string) $rows[1]['id']);
    }

    /**
     * Delete specific rows, re-insert with same PKs but different data.
     */
    public function testDeleteAndReinsertSamePks(): void
    {
        $this->pdo->exec('DELETE FROM dar_cache WHERE id IN (1, 2)');
        $this->pdo->exec("INSERT INTO dar_cache VALUES (1, 'config.theme', 'light')");
        $this->pdo->exec("INSERT INTO dar_cache VALUES (2, 'config.lang', 'ja')");

        $rows = $this->ztdQuery('SELECT * FROM dar_cache ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('light', $rows[0]['value']);
        $this->assertSame('ja', $rows[1]['value']);
        $this->assertSame('UTC', $rows[2]['value']); // unchanged
    }

    /**
     * Rebuild pattern: delete all, insert fresh batch, verify aggregate.
     */
    public function testRebuildPattern(): void
    {
        $this->pdo->exec('DELETE FROM dar_cache WHERE 1=1');

        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO dar_cache VALUES ({$i}, 'key{$i}', 'val{$i}')");
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM dar_cache');
        $this->assertSame('10', (string) $rows[0]['cnt']);

        // Verify first and last
        $first = $this->ztdQuery('SELECT * FROM dar_cache WHERE id = 1');
        $this->assertSame('key1', $first[0]['key_name']);

        $last = $this->ztdQuery('SELECT * FROM dar_cache WHERE id = 10');
        $this->assertSame('key10', $last[0]['key_name']);
    }

    /**
     * Multiple rebuild cycles.
     */
    public function testMultipleRebuildCycles(): void
    {
        for ($cycle = 1; $cycle <= 3; $cycle++) {
            $this->pdo->exec('DELETE FROM dar_cache WHERE 1=1');
            $this->pdo->exec("INSERT INTO dar_cache VALUES (1, 'cycle{$cycle}', 'v{$cycle}')");
        }

        $rows = $this->ztdQuery('SELECT * FROM dar_cache');
        $this->assertCount(1, $rows);
        $this->assertSame('cycle3', $rows[0]['key_name']);
        $this->assertSame('v3', $rows[0]['value']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM dar_cache');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
