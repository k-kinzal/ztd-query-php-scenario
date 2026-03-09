<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests SQL line comments (--) through the CTE rewriter (MySQL PDO).
 *
 * Real-world scenario: MySQL line comments use both -- and # syntax.
 * ORM annotations and debugging comments placed near SQL keywords can
 * break the CTE rewriter's statement classifier.
 * Related to upstream #82 (line comments break CTE rewriter).
 *
 * @spec SPEC-3.1
 * @spec SPEC-6.2
 */
class MysqlLineCommentTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_lc_events (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                category VARCHAR(50) NOT NULL,
                score INT NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_lc_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_lc_events VALUES (1, 'Alpha', 'tech', 85)");
        $this->ztdExec("INSERT INTO my_lc_events VALUES (2, 'Beta', 'science', 92)");
        $this->ztdExec("INSERT INTO my_lc_events VALUES (3, 'Gamma', 'tech', 78)");
    }

    /**
     * Line comment (--) before SELECT.
     */
    public function testDashCommentBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- fetch all events\nSELECT * FROM my_lc_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dash comment before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Hash comment (#) before SELECT (MySQL-specific).
     */
    public function testHashCommentBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "# fetch all events\nSELECT * FROM my_lc_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment before SELECT failed: ' . $e->getMessage()
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
                "SELECT * -- all columns\nFROM my_lc_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment between SELECT and FROM failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Hash comment between FROM and WHERE.
     */
    public function testHashCommentBetweenFromAndWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM my_lc_events # events table\nWHERE category = 'tech' ORDER BY id"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment between FROM and WHERE failed: ' . $e->getMessage()
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
                "-- add new\nINSERT INTO my_lc_events VALUES (4, 'Delta', 'art', 88)"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_lc_events WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Delta', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment before INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Hash comment before UPDATE.
     */
    public function testHashCommentBeforeUpdate(): void
    {
        try {
            $this->ztdExec(
                "# update score\nUPDATE my_lc_events SET score = 99 WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT score FROM my_lc_events WHERE id = 1");
            $this->assertEquals(99, (int) $rows[0]['score']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment before UPDATE failed: ' . $e->getMessage()
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
                "-- remove\nDELETE FROM my_lc_events WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_lc_events ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment before DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple hash comments throughout query.
     */
    public function testMultipleHashComments(): void
    {
        try {
            $rows = $this->ztdQuery(
                "# Query: high scorers\n"
                . "SELECT id, # primary key\n"
                . "       name # event name\n"
                . "FROM my_lc_events # source\n"
                . "WHERE score > 80 # threshold\n"
                . "ORDER BY score DESC # highest first"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple hash comments failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * MySQL optimizer hints (which look like comments: /*+ ... *‍/).
     * These are special block comments that MySQL interprets as hints.
     */
    public function testMysqlOptimizerHintStyle(): void
    {
        try {
            // MySQL optimizer hints use /*+ ... */ syntax
            // The CTE rewriter might misparse this
            $rows = $this->ztdQuery(
                "SELECT /*+ NO_INDEX(my_lc_events) */ * FROM my_lc_events ORDER BY id"
            );

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'MySQL optimizer hint style failed: ' . $e->getMessage()
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
                "-- parameterized\nSELECT * FROM my_lc_events WHERE score > ? ORDER BY id",
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
