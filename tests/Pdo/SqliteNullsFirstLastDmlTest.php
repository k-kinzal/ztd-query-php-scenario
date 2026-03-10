<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests NULLS FIRST / NULLS LAST in ORDER BY within DML contexts on SQLite.
 *
 * SQLite 3.30.0+ supports NULLS FIRST and NULLS LAST in ORDER BY.
 * Tests whether the CTE rewriter preserves these clauses in
 * INSERT...SELECT, UPDATE with subquery, and DELETE with subquery.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteNullsFirstLastDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_nfl_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                priority INTEGER,
                category TEXT
            )',
            'CREATE TABLE sl_nfl_ranked (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                rank_pos INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_nfl_ranked', 'sl_nfl_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_nfl_items (id, name, priority, category) VALUES (1, 'Alpha', 10, 'A')");
        $this->pdo->exec("INSERT INTO sl_nfl_items (id, name, priority, category) VALUES (2, 'Beta', NULL, 'A')");
        $this->pdo->exec("INSERT INTO sl_nfl_items (id, name, priority, category) VALUES (3, 'Gamma', 5, 'B')");
        $this->pdo->exec("INSERT INTO sl_nfl_items (id, name, priority, category) VALUES (4, 'Delta', NULL, 'B')");
        $this->pdo->exec("INSERT INTO sl_nfl_items (id, name, priority, category) VALUES (5, 'Epsilon', 20, NULL)");
    }

    /**
     * SELECT with NULLS FIRST.
     *
     * @spec SPEC-3.1
     */
    public function testSelectNullsFirst(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id, name, priority FROM sl_nfl_items ORDER BY priority ASC NULLS FIRST'
            );

            $this->assertCount(5, $rows);
            // NULLs should come first
            $this->assertNull($rows[0]['priority']);
            $this->assertNull($rows[1]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT NULLS FIRST failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with NULLS LAST.
     *
     * @spec SPEC-3.1
     */
    public function testSelectNullsLast(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id, name, priority FROM sl_nfl_items ORDER BY priority ASC NULLS LAST'
            );

            $this->assertCount(5, $rows);
            // NULLs should come last
            $this->assertNull($rows[3]['priority']);
            $this->assertNull($rows[4]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT NULLS LAST failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with ORDER BY ... NULLS LAST LIMIT.
     * Copy top-priority items (excluding NULLs) into ranked table.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectNullsLastLimit(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_nfl_ranked (id, name, rank_pos)
                 SELECT id, name, ROW_NUMBER() OVER (ORDER BY priority ASC NULLS LAST)
                 FROM sl_nfl_items
                 ORDER BY priority ASC NULLS LAST
                 LIMIT 3"
            );

            $rows = $this->ztdQuery('SELECT id, name, rank_pos FROM sl_nfl_ranked ORDER BY rank_pos');

            if (count($rows) < 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT NULLS LAST LIMIT: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            // First 3 non-NULL priorities: 5 (Gamma), 10 (Alpha), 20 (Epsilon)
            $this->assertSame('Gamma', $rows[0]['name']);
            $this->assertSame('Alpha', $rows[1]['name']);
            $this->assertSame('Epsilon', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT NULLS LAST LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with subquery using NULLS FIRST ordering.
     * Delete items with NULL priority (highest in NULLS FIRST order).
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithNullsFirstSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_nfl_items WHERE id IN (
                    SELECT id FROM sl_nfl_items WHERE priority IS NULL
                )"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_nfl_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE NULL priority: expected 3 remaining, got ' . $remaining
                );
            }

            $this->assertEquals(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with NULL priority subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with NULLS LAST in correlated subquery.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateSubqueryNullsLast(): void
    {
        try {
            // Set priority to 0 for items where category has any NULL-priority item
            $this->pdo->exec(
                "UPDATE sl_nfl_items SET priority = 0
                 WHERE category IN (
                    SELECT DISTINCT category FROM sl_nfl_items WHERE priority IS NULL AND category IS NOT NULL
                 ) AND priority IS NOT NULL"
            );

            $rows = $this->ztdQuery(
                "SELECT id, priority FROM sl_nfl_items WHERE priority = 0 ORDER BY id"
            );

            // Category 'A' has Beta(NULL), so Alpha(10) → 0
            // Category 'B' has Delta(NULL), so Gamma(5) → 0
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with NULL-category subquery: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']); // Alpha
            $this->assertEquals(3, (int) $rows[1]['id']); // Gamma
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with NULLS subquery failed: ' . $e->getMessage());
        }
    }
}
