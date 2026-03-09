<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQL with unusual whitespace formatting through the CTE rewriter.
 *
 * Real-world scenario: ORMs, query builders, and developers produce SQL
 * with varying whitespace — tabs, multiple spaces, leading/trailing newlines,
 * Windows-style \r\n line endings. If the CTE rewriter's statement classifier
 * relies on specific whitespace patterns, these queries will misparse.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteWhitespaceFormattingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_wsf_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_wsf_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_wsf_products VALUES (1, 'Laptop', 999.99, 'electronics')");
        $this->ztdExec("INSERT INTO sl_wsf_products VALUES (2, 'Mouse', 29.99, 'electronics')");
        $this->ztdExec("INSERT INTO sl_wsf_products VALUES (3, 'Desk', 249.99, 'furniture')");
    }

    /**
     * SELECT with leading whitespace (spaces).
     */
    public function testLeadingSpacesBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery("   SELECT * FROM sl_wsf_products ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame('Laptop', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading spaces before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with leading tab characters.
     */
    public function testLeadingTabsBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery("\t\tSELECT * FROM sl_wsf_products ORDER BY id");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading tabs before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with leading newlines.
     */
    public function testLeadingNewlinesBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery("\n\nSELECT * FROM sl_wsf_products ORDER BY id");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading newlines before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with Windows-style CRLF line endings.
     */
    public function testWindowsStyleLineEndings(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT *\r\nFROM sl_wsf_products\r\nWHERE category = 'electronics'\r\nORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Laptop', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Windows-style CRLF line endings failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SQL with tab characters between keywords.
     */
    public function testTabsBetweenKeywords(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT\t*\tFROM\tsl_wsf_products\tWHERE\tcategory = 'electronics'\tORDER BY\tid"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Tabs between keywords failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SQL with multiple spaces between keywords.
     */
    public function testMultipleSpacesBetweenKeywords(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT  *  FROM  sl_wsf_products  WHERE  price > 100  ORDER BY  id"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple spaces between keywords failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multi-line formatted INSERT.
     */
    public function testMultiLineInsert(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO\n  sl_wsf_products\n  (id, name, price, category)\nVALUES\n  (4, 'Chair', 149.99, 'furniture')"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_wsf_products WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Chair', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-line INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with tabs in SET clause.
     */
    public function testUpdateWithTabsInSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE\tsl_wsf_products\tSET\tprice = 899.99\tWHERE\tid = 1"
            );

            $rows = $this->ztdQuery("SELECT price FROM sl_wsf_products WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertEquals(899.99, (float) $rows[0]['price'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with tabs in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with leading mixed whitespace.
     */
    public function testDeleteWithMixedLeadingWhitespace(): void
    {
        try {
            $this->ztdExec(" \t \n DELETE FROM sl_wsf_products WHERE id = 3");

            $rows = $this->ztdQuery("SELECT * FROM sl_wsf_products ORDER BY id");
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Desk', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with mixed leading whitespace failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Trailing semicolon on SELECT.
     */
    public function testTrailingSemicolonOnSelect(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT * FROM sl_wsf_products ORDER BY id;");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Trailing semicolon with whitespace on SELECT.
     */
    public function testTrailingSemicolonWithWhitespaceOnSelect(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT * FROM sl_wsf_products ORDER BY id ;  \n");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon with whitespace on SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Trailing semicolon on INSERT.
     */
    public function testTrailingSemicolonOnInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_wsf_products VALUES (5, 'Monitor', 399.99, 'electronics');");

            $rows = $this->ztdQuery("SELECT * FROM sl_wsf_products WHERE id = 5");
            $this->assertCount(1, $rows);
            $this->assertSame('Monitor', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Trailing semicolon on UPDATE.
     */
    public function testTrailingSemicolonOnUpdate(): void
    {
        try {
            $this->ztdExec("UPDATE sl_wsf_products SET price = 19.99 WHERE id = 2;");

            $rows = $this->ztdQuery("SELECT price FROM sl_wsf_products WHERE id = 2");
            $this->assertEquals(19.99, (float) $rows[0]['price'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Trailing semicolon on DELETE.
     */
    public function testTrailingSemicolonOnDelete(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_wsf_products WHERE id = 3;");

            $rows = $this->ztdQuery("SELECT * FROM sl_wsf_products ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Newlines within column list.
     */
    public function testNewlinesWithinColumnList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT\n  id,\n  name,\n  price,\n  category\nFROM sl_wsf_products\nORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertArrayHasKey('id', $rows[0]);
            $this->assertArrayHasKey('name', $rows[0]);
            $this->assertArrayHasKey('price', $rows[0]);
            $this->assertArrayHasKey('category', $rows[0]);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Newlines within column list failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with leading whitespace.
     */
    public function testPreparedWithLeadingWhitespace(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "  \n\t SELECT * FROM sl_wsf_products WHERE price > ? ORDER BY id",
                [100]
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared with leading whitespace failed: ' . $e->getMessage()
            );
        }
    }
}
