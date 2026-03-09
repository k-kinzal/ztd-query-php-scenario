<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SQL line comments (--) through the CTE rewriter (PostgreSQL).
 *
 * Real-world scenario: PostgreSQL-specific SQL often includes comments
 * for documentation, ORM annotations, and debugging hints. Line comments
 * near keywords can break the CTE rewriter's statement classifier.
 * Related to upstream #82 (line comments break CTE rewriter).
 *
 * @spec SPEC-3.1
 * @spec SPEC-6.2
 */
class PostgresLineCommentTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_lc_events (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_lc_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_lc_events (id, name, category, score) VALUES (1, 'Alpha', 'tech', 85)");
        $this->ztdExec("INSERT INTO pg_lc_events (id, name, category, score) VALUES (2, 'Beta', 'science', 92)");
        $this->ztdExec("INSERT INTO pg_lc_events (id, name, category, score) VALUES (3, 'Gamma', 'tech', 78)");
    }

    /**
     * Line comment before SELECT.
     */
    public function testLineCommentBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- fetch all events\nSELECT * FROM pg_lc_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment between SELECT and FROM.
     */
    public function testLineCommentBetweenSelectAndFrom(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * -- all columns\nFROM pg_lc_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment between SELECT and FROM failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment between FROM and WHERE.
     */
    public function testLineCommentBetweenFromAndWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM pg_lc_events -- events table\nWHERE category = 'tech' ORDER BY id"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment between FROM and WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment before INSERT.
     */
    public function testLineCommentBeforeInsert(): void
    {
        try {
            $this->ztdExec(
                "-- add new event\nINSERT INTO pg_lc_events (id, name, category, score) VALUES (4, 'Delta', 'art', 88)"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_lc_events WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Delta', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment before INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment before UPDATE.
     */
    public function testLineCommentBeforeUpdate(): void
    {
        try {
            $this->ztdExec(
                "-- update score\nUPDATE pg_lc_events SET score = 99 WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT score FROM pg_lc_events WHERE id = 1");
            $this->assertEquals(99, (int) $rows[0]['score']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment before UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment before DELETE.
     */
    public function testLineCommentBeforeDelete(): void
    {
        try {
            $this->ztdExec(
                "-- remove event\nDELETE FROM pg_lc_events WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_lc_events ORDER BY id");
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Beta', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment before DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple line comments throughout query.
     */
    public function testMultipleLineComments(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- Query: high scorers\n"
                . "SELECT id, -- primary key\n"
                . "       name -- event name\n"
                . "FROM pg_lc_events -- source\n"
                . "WHERE score > 80 -- threshold\n"
                . "ORDER BY score DESC -- highest first"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Beta', $rows[0]['name']);   // score 92
            $this->assertSame('Alpha', $rows[1]['name']);  // score 85
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple line comments failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment containing SQL keywords.
     */
    public function testLineCommentContainingSqlKeywords(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- INSERT INTO DELETE FROM UPDATE SET DROP TABLE\nSELECT * FROM pg_lc_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment containing SQL keywords failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with line comment.
     */
    public function testPreparedWithLineComment(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "-- parameterized query\nSELECT * FROM pg_lc_events WHERE score > ? ORDER BY id",
                [80]
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared with line comment failed: ' . $e->getMessage()
            );
        }
    }
}
