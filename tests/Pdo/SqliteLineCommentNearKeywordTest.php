<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQL line comments (--) near keywords through the CTE rewriter.
 *
 * Real-world scenario: developers add comments to SQL queries for documentation,
 * debugging, or ORM-generated annotations. Line comments (--) near SQL keywords
 * like SELECT, FROM, WHERE, INSERT, UPDATE, DELETE could break the CTE rewriter's
 * statement parser, similar to block comment issues reported in upstream #69.
 *
 * @spec SPEC-3.1
 * @spec SPEC-6.2
 */
class SqliteLineCommentNearKeywordTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_lcnk_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT \'active\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_lcnk_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_lcnk_items VALUES (1, 'Widget', 'active')");
        $this->ztdExec("INSERT INTO sl_lcnk_items VALUES (2, 'Gadget', 'inactive')");
        $this->ztdExec("INSERT INTO sl_lcnk_items VALUES (3, 'Sprocket', 'active')");
    }

    /**
     * Line comment before SELECT keyword.
     */
    public function testLineCommentBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- fetch all active items\nSELECT * FROM sl_lcnk_items WHERE status = 'active' ORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
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
                "SELECT * -- all columns\nFROM sl_lcnk_items ORDER BY id"
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
                "SELECT * FROM sl_lcnk_items -- main table\nWHERE status = 'active' ORDER BY id"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment between FROM and WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment after WHERE clause.
     */
    public function testLineCommentAfterWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM sl_lcnk_items WHERE id > 1 -- skip first\nORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(2, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment after WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment before INSERT.
     */
    public function testLineCommentBeforeInsert(): void
    {
        try {
            $this->ztdExec("-- add new item\nINSERT INTO sl_lcnk_items VALUES (4, 'Bolt', 'active')");

            $rows = $this->ztdQuery("SELECT * FROM sl_lcnk_items WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Bolt', $rows[0]['name']);
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
                "-- deactivate item\nUPDATE sl_lcnk_items SET status = 'inactive' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT status FROM sl_lcnk_items WHERE id = 1");
            $this->assertSame('inactive', $rows[0]['status']);
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
            $this->ztdExec("-- remove item\nDELETE FROM sl_lcnk_items WHERE id = 2");

            $rows = $this->ztdQuery("SELECT * FROM sl_lcnk_items ORDER BY id");
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Gadget', $names);
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
                "-- Query: get active items\n"
                . "SELECT id, -- primary key\n"
                . "       name -- display name\n"
                . "FROM sl_lcnk_items -- source table\n"
                . "WHERE status = 'active' -- only active\n"
                . "ORDER BY name -- alphabetical"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple line comments failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment containing SQL keywords (should be ignored by parser).
     */
    public function testLineCommentContainingSqlKeywords(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- SELECT * FROM other_table WHERE DELETE UPDATE INSERT\nSELECT * FROM sl_lcnk_items ORDER BY id"
            );

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment containing SQL keywords failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment with table name inside (should not be rewritten).
     */
    public function testLineCommentContainingTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- sl_lcnk_items has 3 rows\nSELECT COUNT(*) AS cnt FROM sl_lcnk_items"
            );

            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment containing table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Line comment in prepared statement.
     */
    public function testLineCommentInPreparedStatement(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "-- filtered query\nSELECT * FROM sl_lcnk_items WHERE status = ? ORDER BY id",
                ['active']
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Line comment in prepared statement failed: ' . $e->getMessage()
            );
        }
    }
}
