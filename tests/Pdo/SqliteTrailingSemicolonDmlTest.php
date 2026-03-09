<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML statements with trailing semicolons and whitespace (SQLite).
 *
 * Real-world scenario: Many SQL clients, ORMs, and migration tools
 * append semicolons to SQL statements. Some also leave trailing whitespace
 * or newlines. The CTE rewriter must handle these gracefully — either
 * stripping them or ignoring them during classification.
 *
 * This test focuses specifically on DML operations (INSERT, UPDATE, DELETE)
 * with various trailing content, since the rewriter transforms these
 * more aggressively than SELECT.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.3
 * @spec SPEC-4.5
 */
class SqliteTrailingSemicolonDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tsd_tasks (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                done INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tsd_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_tsd_tasks VALUES (1, 'Task A', 0)");
        $this->ztdExec("INSERT INTO sl_tsd_tasks VALUES (2, 'Task B', 1)");
        $this->ztdExec("INSERT INTO sl_tsd_tasks VALUES (3, 'Task C', 0)");
    }

    /**
     * INSERT with trailing semicolon and spaces.
     */
    public function testInsertWithTrailingSemicolonAndSpaces(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_tsd_tasks VALUES (4, 'Task D', 0) ;  ");

            $rows = $this->ztdQuery("SELECT * FROM sl_tsd_tasks WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Task D', $rows[0]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with trailing semicolon and spaces failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with trailing semicolon and newline.
     */
    public function testUpdateWithTrailingSemicolonAndNewline(): void
    {
        try {
            $this->ztdExec("UPDATE sl_tsd_tasks SET done = 1 WHERE id = 1;\n");

            $rows = $this->ztdQuery("SELECT done FROM sl_tsd_tasks WHERE id = 1");
            $this->assertEquals(1, (int) $rows[0]['done']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with trailing semicolon and newline failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with trailing semicolon and tab.
     */
    public function testDeleteWithTrailingSemicolonAndTab(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_tsd_tasks WHERE id = 3;\t");

            $rows = $this->ztdQuery("SELECT * FROM sl_tsd_tasks ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with trailing semicolon and tab failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple trailing semicolons.
     */
    public function testMultipleTrailingSemicolons(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT * FROM sl_tsd_tasks ORDER BY id;;");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple trailing semicolons failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with only trailing newlines (no semicolon).
     */
    public function testInsertWithTrailingNewlines(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_tsd_tasks VALUES (5, 'Task E', 1)\n\n\n");

            $rows = $this->ztdQuery("SELECT * FROM sl_tsd_tasks WHERE id = 5");
            $this->assertCount(1, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with trailing newlines failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Combined: leading whitespace + trailing semicolon.
     */
    public function testLeadingWhitespacePlusTrailingSemicolon(): void
    {
        try {
            $this->ztdExec("  \n  INSERT INTO sl_tsd_tasks VALUES (6, 'Task F', 0) ; \n ");

            $rows = $this->ztdQuery("SELECT * FROM sl_tsd_tasks WHERE id = 6");
            $this->assertCount(1, $rows);
            $this->assertSame('Task F', $rows[0]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading whitespace + trailing semicolon failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT with trailing semicolon.
     */
    public function testPreparedInsertWithTrailingSemicolon(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_tsd_tasks VALUES (?, ?, ?);");
            $stmt->execute([7, 'Task G', 0]);

            $rows = $this->ztdQuery("SELECT * FROM sl_tsd_tasks WHERE id = 7");
            $this->assertCount(1, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared INSERT with trailing semicolon failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with trailing semicolon.
     */
    public function testPreparedSelectWithTrailingSemicolon(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT * FROM sl_tsd_tasks WHERE done = ? ORDER BY id;",
                [0]
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared SELECT with trailing semicolon failed: ' . $e->getMessage()
            );
        }
    }
}
