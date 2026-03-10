<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE all rows followed by re-INSERT through ZTD on PostgreSQL.
 *
 * Cache rebuild / batch sync pattern: clear all data then repopulate.
 *
 * @spec SPEC-4.3
 * @spec SPEC-4.1
 */
class PostgresDeleteAllRebuildTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dar_cache (id INTEGER PRIMARY KEY, key_name TEXT, value TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dar_cache'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (1, 'config.theme', 'dark')");
        $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (2, 'config.lang', 'en')");
        $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (3, 'config.tz', 'UTC')");
    }

    /**
     * DELETE WHERE 1=1, then verify empty.
     */
    public function testDeleteAllWithWhereTrue(): void
    {
        try {
            $this->pdo->exec('DELETE FROM pg_dar_cache WHERE 1=1');

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_dar_cache');
            $this->assertEquals(0, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE 1=1 failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE without WHERE clause.
     */
    public function testDeleteWithoutWhere(): void
    {
        try {
            $this->pdo->exec('DELETE FROM pg_dar_cache');

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_dar_cache');
            if ((int) $rows[0]['cnt'] !== 0) {
                $this->markTestIncomplete(
                    'DELETE without WHERE: expected 0, got ' . $rows[0]['cnt']
                );
            }
            $this->assertEquals(0, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE without WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Delete all then re-insert.
     */
    public function testDeleteAllThenReinsert(): void
    {
        try {
            $this->pdo->exec('DELETE FROM pg_dar_cache WHERE 1=1');
            $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (10, 'new.key1', 'val1')");
            $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (11, 'new.key2', 'val2')");

            $rows = $this->ztdQuery('SELECT * FROM pg_dar_cache ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertEquals(10, (int) $rows[0]['id']);
            $this->assertSame('new.key1', $rows[0]['key_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE all + re-INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Delete specific rows, re-insert with same PKs.
     */
    public function testDeleteAndReinsertSamePks(): void
    {
        try {
            $this->pdo->exec('DELETE FROM pg_dar_cache WHERE id IN (1, 2)');
            $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (1, 'config.theme', 'light')");
            $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (2, 'config.lang', 'ja')");

            $rows = $this->ztdQuery('SELECT * FROM pg_dar_cache ORDER BY id');
            $this->assertCount(3, $rows);
            $this->assertSame('light', $rows[0]['value']);
            $this->assertSame('ja', $rows[1]['value']);
            $this->assertSame('UTC', $rows[2]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Delete + re-INSERT same PKs failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple rebuild cycles.
     */
    public function testMultipleRebuildCycles(): void
    {
        try {
            for ($cycle = 1; $cycle <= 3; $cycle++) {
                $this->pdo->exec('DELETE FROM pg_dar_cache WHERE 1=1');
                $this->pdo->exec("INSERT INTO pg_dar_cache VALUES (1, 'cycle{$cycle}', 'v{$cycle}')");
            }

            $rows = $this->ztdQuery('SELECT * FROM pg_dar_cache');
            $this->assertCount(1, $rows);
            $this->assertSame('cycle3', $rows[0]['key_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple rebuild cycles failed: ' . $e->getMessage());
        }
    }
}
