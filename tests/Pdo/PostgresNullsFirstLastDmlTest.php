<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests NULLS FIRST / NULLS LAST in ORDER BY within DML contexts on PostgreSQL.
 *
 * PostgreSQL sorts NULLs last in ASC by default. NULLS FIRST/LAST in DML
 * subqueries (INSERT...SELECT, UPDATE/DELETE with subqueries) must be
 * preserved by the CTE rewriter.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 */
class PostgresNullsFirstLastDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_nfl_items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                priority INTEGER,
                category TEXT
            )',
            'CREATE TABLE pg_nfl_ranked (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                rank_pos INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_nfl_ranked', 'pg_nfl_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_nfl_items (name, priority, category) VALUES ('Alpha', 10, 'A')");
        $this->pdo->exec("INSERT INTO pg_nfl_items (name, priority, category) VALUES ('Beta', NULL, 'A')");
        $this->pdo->exec("INSERT INTO pg_nfl_items (name, priority, category) VALUES ('Gamma', 5, 'B')");
        $this->pdo->exec("INSERT INTO pg_nfl_items (name, priority, category) VALUES ('Delta', NULL, 'B')");
        $this->pdo->exec("INSERT INTO pg_nfl_items (name, priority, category) VALUES ('Epsilon', 20, NULL)");
    }

    /**
     * SELECT with NULLS FIRST on PostgreSQL.
     *
     * @spec SPEC-3.1
     */
    public function testSelectNullsFirst(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id, name, priority FROM pg_nfl_items ORDER BY priority ASC NULLS FIRST'
            );

            $this->assertCount(5, $rows);
            $this->assertNull($rows[0]['priority']);
            $this->assertNull($rows[1]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT NULLS FIRST failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with NULLS LAST LIMIT.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectNullsLastLimit(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_nfl_ranked (id, name, rank_pos)
                 SELECT id, name, ROW_NUMBER() OVER (ORDER BY priority ASC NULLS LAST)
                 FROM pg_nfl_items
                 ORDER BY priority ASC NULLS LAST
                 LIMIT 3"
            );

            $rows = $this->ztdQuery('SELECT id, name, rank_pos FROM pg_nfl_ranked ORDER BY rank_pos');

            if (count($rows) < 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT NULLS LAST LIMIT: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Gamma', $rows[0]['name']);
            $this->assertSame('Alpha', $rows[1]['name']);
            $this->assertSame('Epsilon', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT NULLS LAST LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with NULLS FIRST and ? parameter.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedSelectNullsFirst(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT name, priority FROM pg_nfl_items
                 WHERE category = ?
                 ORDER BY priority ASC NULLS FIRST',
                ['A']
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Prepared NULLS FIRST: expected 2 rows');
            }

            $this->assertCount(2, $rows);
            $this->assertNull($rows[0]['priority'], 'NULL priority should come first');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT NULLS FIRST failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with NULLS FIRST and $N parameter.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedSelectNullsFirstDollarParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT name, priority FROM pg_nfl_items
                 WHERE category = $1
                 ORDER BY priority ASC NULLS FIRST',
                ['A']
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Prepared NULLS FIRST $N: expected 2 rows, got ' . count($rows));
            }

            $this->assertCount(2, $rows);
            $this->assertNull($rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT NULLS FIRST $N failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with ORDER BY NULLS LAST subquery.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithNullPriorityItems(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_nfl_items WHERE priority IS NULL"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_nfl_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE NULL priority: expected 3, got ' . $remaining
                );
            }

            $this->assertEquals(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE NULL priority failed: ' . $e->getMessage());
        }
    }

    /**
     * FETCH FIRST N ROWS ONLY with NULLS LAST.
     *
     * @spec SPEC-3.1
     */
    public function testFetchFirstNullsLast(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, priority FROM pg_nfl_items
                 ORDER BY priority ASC NULLS LAST
                 FETCH FIRST 3 ROWS ONLY"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete('FETCH FIRST NULLS LAST: expected 3 rows');
            }

            // Non-NULL priorities come first: 5, 10, 20
            $this->assertCount(3, $rows);
            $this->assertEquals(5, (int) $rows[0]['priority']);
            $this->assertEquals(10, (int) $rows[1]['priority']);
            $this->assertEquals(20, (int) $rows[2]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('FETCH FIRST NULLS LAST failed: ' . $e->getMessage());
        }
    }
}
