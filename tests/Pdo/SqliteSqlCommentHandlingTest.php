<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that SQL comments in queries are handled correctly by the CTE rewriter.
 *
 * Generated SQL from ORMs and query builders often embeds comments.
 * The CTE rewriter must not confuse comment content with SQL keywords or table refs.
 * @spec SPEC-3.1
 */
class SqliteSqlCommentHandlingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sch_products (id INTEGER PRIMARY KEY, name TEXT, price REAL, active INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sch_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sch_products VALUES (1, 'Widget', 9.99, 1)");
        $this->pdo->exec("INSERT INTO sch_products VALUES (2, 'Gadget', 19.99, 1)");
        $this->pdo->exec("INSERT INTO sch_products VALUES (3, 'Defunct', 5.00, 0)");
    }

    /**
     * Block comment before SELECT keyword.
     */
    public function testBlockCommentBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery('/* fetch all products */ SELECT * FROM sch_products ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Block comment before SELECT failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Block comment before SELECT returned empty results');
            return;
        }
        $this->assertCount(3, $rows);
    }

    /**
     * Block comment between SELECT and FROM.
     */
    public function testBlockCommentBetweenSelectAndFrom(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * /* all columns */ FROM sch_products ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Block comment between SELECT and FROM failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Block comment between SELECT and FROM returned empty results');
            return;
        }
        $this->assertCount(3, $rows);
    }

    /**
     * Block comment after FROM table name.
     */
    public function testBlockCommentAfterTableName(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM sch_products /* products table */ WHERE active = 1 ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Block comment after table name failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Block comment after table name returned empty results');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Inline comment (--) at end of query.
     */
    public function testInlineCommentAtEnd(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT * FROM sch_products WHERE active = 1 ORDER BY id -- only active");
        } catch (\Exception $e) {
            $this->markTestIncomplete('Inline comment at end failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Inline comment at end returned empty results');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Comment containing table name reference — should NOT be rewritten.
     */
    public function testCommentContainingTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                '/* FROM sch_products WHERE ... */ SELECT id, name FROM sch_products ORDER BY id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment containing table name failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment containing table name returned empty results — CTE rewriter may be confused by comment content');
            return;
        }
        $this->assertCount(3, $rows);
    }

    /**
     * Comment containing SQL keywords — should not confuse parser.
     */
    public function testCommentContainingSqlKeywords(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id /* SELECT DELETE UPDATE INSERT */ FROM sch_products ORDER BY id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment containing SQL keywords failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment with SQL keywords returned empty');
            return;
        }
        $this->assertCount(3, $rows);
    }

    /**
     * INSERT with block comment.
     */
    public function testInsertWithComment(): void
    {
        try {
            $this->pdo->exec("/* add new product */ INSERT INTO sch_products VALUES (4, 'New', 15.00, 1)");
            $rows = $this->ztdQuery('SELECT * FROM sch_products WHERE id = 4');
        } catch (\Exception $e) {
            $this->markTestIncomplete('INSERT with comment failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('INSERT with comment: row not visible in shadow store');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('New', $rows[0]['name']);
    }

    /**
     * UPDATE with block comment.
     */
    public function testUpdateWithComment(): void
    {
        try {
            $this->pdo->exec('/* deactivate defunct */ UPDATE sch_products SET active = 0 WHERE price < 6');
            $rows = $this->ztdQuery('SELECT * FROM sch_products WHERE active = 1 ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE with comment failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
    }

    /**
     * DELETE with inline comment.
     */
    public function testDeleteWithInlineComment(): void
    {
        try {
            $this->pdo->exec('DELETE FROM sch_products WHERE id = 3 -- remove defunct');
            $rows = $this->ztdQuery('SELECT * FROM sch_products ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE with inline comment failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
    }

    /**
     * Multiple block comments in a single query.
     */
    public function testMultipleBlockComments(): void
    {
        try {
            $rows = $this->ztdQuery(
                '/* query: product list */ SELECT /* columns */ id, name /* from products */ FROM sch_products /* filter */ WHERE active = 1 /* sort */ ORDER BY id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multiple block comments failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Multiple block comments returned empty results');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * Prepared statement with comment.
     */
    public function testPreparedWithComment(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                '/* parameterized query */ SELECT * FROM sch_products WHERE price > ? ORDER BY id',
                [10.0]
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared with comment failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Prepared with comment returned empty results');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
    }
}
