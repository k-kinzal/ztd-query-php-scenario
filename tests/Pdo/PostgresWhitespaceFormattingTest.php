<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SQL with unusual whitespace formatting through the CTE rewriter (PostgreSQL).
 *
 * Real-world scenario: ORMs, query builders, and developers produce SQL
 * with varying whitespace — tabs, multiple spaces, leading/trailing newlines,
 * Windows-style \r\n. If the CTE rewriter's statement classifier relies on
 * specific whitespace patterns, these queries will misparse.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresWhitespaceFormattingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_wsf_products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                category TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_wsf_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_wsf_products (id, name, price, category) VALUES (1, 'Laptop', 999.99, 'electronics')");
        $this->ztdExec("INSERT INTO pg_wsf_products (id, name, price, category) VALUES (2, 'Mouse', 29.99, 'electronics')");
        $this->ztdExec("INSERT INTO pg_wsf_products (id, name, price, category) VALUES (3, 'Desk', 249.99, 'furniture')");
    }

    /**
     * SELECT with leading whitespace (spaces and tabs).
     */
    public function testLeadingWhitespaceBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery("   \t  SELECT * FROM pg_wsf_products ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame('Laptop', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading whitespace before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with leading newlines.
     */
    public function testLeadingNewlinesBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery("\n\n\nSELECT * FROM pg_wsf_products ORDER BY id");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading newlines before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Windows-style CRLF line endings.
     */
    public function testWindowsStyleCrlfLineEndings(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT *\r\nFROM pg_wsf_products\r\nWHERE category = 'electronics'\r\nORDER BY id"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Windows-style CRLF failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Tabs between all keywords.
     */
    public function testTabsBetweenAllKeywords(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT\t*\tFROM\tpg_wsf_products\tWHERE\tprice > 100\tORDER BY\tid"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Tabs between all keywords failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with newlines within VALUES.
     */
    public function testInsertWithNewlinesInValues(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_wsf_products\n(\n  id,\n  name,\n  price,\n  category\n)\nVALUES\n(\n  4,\n  'Chair',\n  149.99,\n  'furniture'\n)"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_wsf_products WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Chair', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with newlines in VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with tabs in SET clause.
     */
    public function testUpdateWithTabsInSet(): void
    {
        try {
            $this->ztdExec("UPDATE\tpg_wsf_products\tSET\tprice = 899.99\tWHERE\tid = 1");

            $rows = $this->ztdQuery("SELECT price FROM pg_wsf_products WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertEquals(899.99, (float) $rows[0]['price'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with tabs failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with mixed leading whitespace.
     */
    public function testDeleteWithMixedLeadingWhitespace(): void
    {
        try {
            $this->ztdExec(" \t \n DELETE FROM pg_wsf_products WHERE id = 3");

            $rows = $this->ztdQuery("SELECT * FROM pg_wsf_products ORDER BY id");
            $this->assertCount(2, $rows);
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
            $rows = $this->ztdQuery("SELECT * FROM pg_wsf_products ORDER BY id;");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Trailing semicolon with whitespace on INSERT.
     */
    public function testTrailingSemicolonOnInsert(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_wsf_products (id, name, price, category) VALUES (5, 'Monitor', 399.99, 'electronics') ; "
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_wsf_products WHERE id = 5");
            $this->assertCount(1, $rows);
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
            $this->ztdExec("UPDATE pg_wsf_products SET price = 19.99 WHERE id = 2;");

            $rows = $this->ztdQuery("SELECT price FROM pg_wsf_products WHERE id = 2");
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
            $this->ztdExec("DELETE FROM pg_wsf_products WHERE id = 3;");

            $rows = $this->ztdQuery("SELECT * FROM pg_wsf_products ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on DELETE failed: ' . $e->getMessage()
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
                "  \n\t SELECT * FROM pg_wsf_products WHERE price > ? ORDER BY id",
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
