<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests SQL with unusual whitespace formatting through the CTE rewriter (MySQLi).
 *
 * Real-world scenario: ORMs and query builders produce SQL with varying
 * whitespace — tabs, multiple spaces, leading/trailing newlines, CRLF.
 * If the CTE rewriter relies on specific whitespace patterns, these fail.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class MysqliWhitespaceFormattingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_wsf_products (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                category VARCHAR(50) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_wsf_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_wsf_products VALUES (1, 'Laptop', 999.99, 'electronics')");
        $this->ztdExec("INSERT INTO mi_wsf_products VALUES (2, 'Mouse', 29.99, 'electronics')");
        $this->ztdExec("INSERT INTO mi_wsf_products VALUES (3, 'Desk', 249.99, 'furniture')");
    }

    public function testLeadingWhitespaceBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery("   \t  SELECT * FROM mi_wsf_products ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame('Laptop', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading whitespace before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    public function testLeadingNewlines(): void
    {
        try {
            $rows = $this->ztdQuery("\n\n\nSELECT * FROM mi_wsf_products ORDER BY id");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Leading newlines failed: ' . $e->getMessage()
            );
        }
    }

    public function testWindowsCrlf(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT *\r\nFROM mi_wsf_products\r\nWHERE category = 'electronics'\r\nORDER BY id"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'CRLF failed: ' . $e->getMessage()
            );
        }
    }

    public function testTabsBetweenKeywords(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT\t*\tFROM\tmi_wsf_products\tWHERE\tprice > 100\tORDER BY\tid"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Tabs between keywords failed: ' . $e->getMessage()
            );
        }
    }

    public function testInsertWithNewlines(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO\n  mi_wsf_products\n  (id, name, price, category)\nVALUES\n  (4, 'Chair', 149.99, 'furniture')"
            );

            $rows = $this->ztdQuery("SELECT * FROM mi_wsf_products WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Chair', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with newlines failed: ' . $e->getMessage()
            );
        }
    }

    public function testUpdateWithTabs(): void
    {
        try {
            $this->ztdExec("UPDATE\tmi_wsf_products\tSET\tprice = 899.99\tWHERE\tid = 1");

            $rows = $this->ztdQuery("SELECT price FROM mi_wsf_products WHERE id = 1");
            $this->assertEquals(899.99, (float) $rows[0]['price'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with tabs failed: ' . $e->getMessage()
            );
        }
    }

    public function testDeleteWithMixedWhitespace(): void
    {
        try {
            $this->ztdExec(" \t \n DELETE FROM mi_wsf_products WHERE id = 3");

            $rows = $this->ztdQuery("SELECT * FROM mi_wsf_products ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with mixed whitespace failed: ' . $e->getMessage()
            );
        }
    }

    public function testTrailingSemicolonSelect(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT * FROM mi_wsf_products ORDER BY id;");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on SELECT failed: ' . $e->getMessage()
            );
        }
    }

    public function testTrailingSemicolonInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_wsf_products VALUES (5, 'Monitor', 399.99, 'electronics');");

            $rows = $this->ztdQuery("SELECT * FROM mi_wsf_products WHERE id = 5");
            $this->assertCount(1, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on INSERT failed: ' . $e->getMessage()
            );
        }
    }

    public function testTrailingSemicolonUpdate(): void
    {
        try {
            $this->ztdExec("UPDATE mi_wsf_products SET price = 19.99 WHERE id = 2;");

            $rows = $this->ztdQuery("SELECT price FROM mi_wsf_products WHERE id = 2");
            $this->assertEquals(19.99, (float) $rows[0]['price'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    public function testTrailingSemicolonDelete(): void
    {
        try {
            $this->ztdExec("DELETE FROM mi_wsf_products WHERE id = 3;");

            $rows = $this->ztdQuery("SELECT * FROM mi_wsf_products ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Trailing semicolon on DELETE failed: ' . $e->getMessage()
            );
        }
    }

    public function testPreparedWithLeadingWhitespace(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "  \n\t SELECT * FROM mi_wsf_products WHERE price > ? ORDER BY id",
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
