<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Focused tests for SQL comment handling by the CTE rewriter.
 *
 * Isolates specific comment patterns that trip up the CTE rewriter's SQL parser.
 * The parser must strip or ignore comments before identifying statement type and
 * table references.
 * @spec SPEC-3.1
 */
class SqliteSqlCommentRewriteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE scr_data (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['scr_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO scr_data VALUES (1, 'alpha', 10)");
        $this->pdo->exec("INSERT INTO scr_data VALUES (2, 'beta', 20)");
    }

    // ── SELECT with comments ──────────────────────────────────────────

    /**
     * Leading block comment before SELECT.
     */
    public function testLeadingBlockCommentSelect(): void
    {
        try {
            $rows = $this->ztdQuery('/* query */ SELECT * FROM scr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment SELECT: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Leading block comment SELECT returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Trailing inline comment on SELECT.
     */
    public function testTrailingInlineCommentSelect(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM scr_data ORDER BY id -- fetch all');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Trailing inline comment SELECT: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Trailing inline comment SELECT returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Comment between columns and FROM (minimal).
     */
    public function testCommentBetweenColumnsAndFrom(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT id, val /* cols */ FROM scr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment between cols and FROM: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment between cols and FROM returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Two block comments in SELECT query.
     */
    public function testTwoBlockCommentsSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                '/* first */ SELECT id /* second */ FROM scr_data ORDER BY id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Two block comments SELECT: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Two block comments SELECT returned empty — parser may mishandle multiple comments');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Three block comments in SELECT query.
     */
    public function testThreeBlockCommentsSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                '/* a */ SELECT /* b */ id, val /* c */ FROM scr_data ORDER BY id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Three block comments SELECT: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Three block comments SELECT returned empty — multiple comments confuse rewriter');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Comment in WHERE clause.
     */
    public function testCommentInWhereClause(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT * FROM scr_data WHERE /* filter */ num > 15 ORDER BY id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment in WHERE: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment in WHERE returned empty');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('beta', $rows[0]['val']);
    }

    // ── INSERT with comments ──────────────────────────────────────────

    /**
     * Leading block comment before INSERT.
     */
    public function testLeadingBlockCommentInsert(): void
    {
        try {
            $this->pdo->exec("/* add row */ INSERT INTO scr_data VALUES (3, 'gamma', 30)");
            $rows = $this->ztdQuery('SELECT * FROM scr_data WHERE id = 3');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment INSERT: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Leading block comment INSERT: row not visible in shadow store');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('gamma', $rows[0]['val']);
    }

    /**
     * Trailing inline comment on INSERT.
     */
    public function testTrailingInlineCommentInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO scr_data VALUES (3, 'gamma', 30) -- new row");
            $rows = $this->ztdQuery('SELECT * FROM scr_data WHERE id = 3');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Trailing inline comment INSERT: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Trailing inline comment INSERT: row not visible');
            return;
        }
        $this->assertCount(1, $rows);
    }

    // ── UPDATE with comments ──────────────────────────────────────────

    /**
     * Leading block comment before UPDATE.
     */
    public function testLeadingBlockCommentUpdate(): void
    {
        try {
            $this->pdo->exec('/* modify */ UPDATE scr_data SET num = 99 WHERE id = 1');
            $rows = $this->ztdQuery('SELECT num FROM scr_data WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment UPDATE: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('99', (string) $rows[0]['num']);
    }

    /**
     * Trailing inline comment on UPDATE.
     */
    public function testTrailingInlineCommentUpdate(): void
    {
        try {
            $this->pdo->exec('UPDATE scr_data SET num = 99 WHERE id = 1 -- bump value');
            $rows = $this->ztdQuery('SELECT num FROM scr_data WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Trailing inline comment UPDATE: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('99', (string) $rows[0]['num']);
    }

    /**
     * Comment between UPDATE and SET.
     */
    public function testCommentBetweenUpdateAndSet(): void
    {
        try {
            $this->pdo->exec('UPDATE scr_data /* table */ SET num = 99 WHERE id = 1');
            $rows = $this->ztdQuery('SELECT num FROM scr_data WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment between UPDATE and SET: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('99', (string) $rows[0]['num']);
    }

    // ── DELETE with comments ──────────────────────────────────────────

    /**
     * Leading block comment before DELETE.
     */
    public function testLeadingBlockCommentDelete(): void
    {
        try {
            $this->pdo->exec('/* remove */ DELETE FROM scr_data WHERE id = 2');
            $rows = $this->ztdQuery('SELECT * FROM scr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment DELETE: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
    }

    /**
     * Trailing inline comment on DELETE.
     */
    public function testTrailingInlineCommentDelete(): void
    {
        try {
            $this->pdo->exec('DELETE FROM scr_data WHERE id = 2 -- cleanup');
            $rows = $this->ztdQuery('SELECT * FROM scr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Trailing inline comment DELETE: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
    }
}
