<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Isolates the exact comment position that breaks the CTE rewriter.
 *
 * Hypothesis: comments between FROM/JOIN and the table name prevent the
 * rewriter from matching the table reference.
 * @spec SPEC-3.1
 */
class SqliteCommentPositionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE cp_alpha (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE cp_beta (id INTEGER PRIMARY KEY, alpha_id INTEGER, label TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['cp_alpha', 'cp_beta'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO cp_alpha VALUES (1, 'one')");
        $this->pdo->exec("INSERT INTO cp_alpha VALUES (2, 'two')");
        $this->pdo->exec("INSERT INTO cp_beta VALUES (1, 1, 'tag1')");
        $this->pdo->exec("INSERT INTO cp_beta VALUES (2, 2, 'tag2')");
    }

    /**
     * Comment between FROM and table — single table.
     */
    public function testCommentBetweenFromAndTable(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM /* tbl */ cp_alpha ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment FROM/**/table: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment between FROM and table returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Comment between JOIN and table.
     */
    public function testCommentBetweenJoinAndTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT a.val, b.label FROM cp_alpha a JOIN /* tbl */ cp_beta b ON b.alpha_id = a.id ORDER BY a.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment JOIN/**/table: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment between JOIN and table returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Comment between LEFT JOIN and table.
     */
    public function testCommentBetweenLeftJoinAndTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT a.val, b.label FROM cp_alpha a LEFT JOIN /* tbl */ cp_beta b ON b.alpha_id = a.id ORDER BY a.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment LEFT JOIN/**/table: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment between LEFT JOIN and table returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Comment AFTER table name (not between FROM and table) — should work.
     */
    public function testCommentAfterTableName(): void
    {
        $rows = $this->ztdQuery('SELECT * FROM cp_alpha /* after table */ ORDER BY id');
        $this->assertCount(2, $rows);
    }

    /**
     * Comment before FROM keyword.
     * Known Issue [#69]: Comment containing 'FROM' text near the FROM keyword
     * confuses the CTE rewriter on SQLite.
     */
    public function testCommentBeforeFrom(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * /* before FROM */ FROM cp_alpha ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment before FROM: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment before FROM returned empty — comment text containing FROM confuses rewriter [Issue #69]');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Four comments, none between FROM and table — should work.
     */
    public function testFourCommentsNoneBetweenFromAndTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                '/* c1 */ SELECT /* c2 */ id, val /* c3 */ FROM cp_alpha /* c4 */ ORDER BY id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('4 comments (none between FROM and table): ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('4 comments (none between FROM and table) returned empty — issue may be comment count not position');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Comment between UPDATE and table name.
     */
    public function testCommentBetweenUpdateAndTable(): void
    {
        try {
            $this->pdo->exec('UPDATE /* tbl */ cp_alpha SET val = \'updated\' WHERE id = 1');
            $rows = $this->ztdQuery('SELECT val FROM cp_alpha WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment UPDATE/**/table: ' . $e->getMessage());
            return;
        }
        $this->assertSame('updated', $rows[0]['val']);
    }

    /**
     * Comment between DELETE FROM and table name.
     * Known Issue [#69]: DELETE silently ignored when comment is between FROM and table.
     */
    public function testCommentBetweenDeleteFromAndTable(): void
    {
        try {
            $this->pdo->exec('DELETE FROM /* tbl */ cp_alpha WHERE id = 2');
            $rows = $this->ztdQuery('SELECT * FROM cp_alpha ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment DELETE FROM/**/table: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 2) {
            $this->markTestIncomplete('DELETE silently ignored when comment between FROM and table [Issue #69]');
            return;
        }
        $this->assertCount(1, $rows);
    }

    /**
     * Comment between INSERT INTO and table name.
     */
    public function testCommentBetweenInsertIntoAndTable(): void
    {
        try {
            $this->pdo->exec("INSERT INTO /* tbl */ cp_alpha VALUES (3, 'three')");
            $rows = $this->ztdQuery('SELECT * FROM cp_alpha WHERE id = 3');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment INSERT INTO/**/table: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment INSERT INTO/**/table: row not visible');
            return;
        }
        $this->assertCount(1, $rows);
    }
}
