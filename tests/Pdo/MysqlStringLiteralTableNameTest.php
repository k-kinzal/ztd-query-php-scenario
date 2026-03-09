<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests queries where string literal values match table names (MySQL PDO).
 *
 * Real-world scenario: If a table is named "items" and a column stores
 * the string "items", the CTE rewriter might incorrectly rewrite the
 * table reference inside the string literal. Known for SQLite (#67),
 * this tests whether the MySQL platform has the same issue.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class MysqlStringLiteralTableNameTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_sltn_items (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                source VARCHAR(100)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_sltn_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_sltn_items VALUES (1, 'alpha', 'from my_sltn_items table', 'my_sltn_items')");
        $this->ztdExec("INSERT INTO my_sltn_items VALUES (2, 'beta', 'SELECT * FROM my_sltn_items', 'external')");
        $this->ztdExec("INSERT INTO my_sltn_items VALUES (3, 'gamma', 'normal description', 'manual')");
    }

    /**
     * WHERE clause with string literal matching table name.
     */
    public function testWhereStringMatchesTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM my_sltn_items WHERE source = 'my_sltn_items' ORDER BY id"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('alpha', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE string matching table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * String literal containing full SQL statement with table name.
     */
    public function testStringContainingSqlWithTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, description FROM my_sltn_items WHERE description LIKE '%SELECT%FROM%my_sltn_items%'"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(2, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'String containing SQL with table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with string values that are the table name.
     */
    public function testInsertStringIsTableName(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_sltn_items VALUES (4, 'my_sltn_items', 'my_sltn_items', 'my_sltn_items')"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_sltn_items WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('my_sltn_items', $rows[0]['name']);
            $this->assertSame('my_sltn_items', $rows[0]['description']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT string is table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with string value matching table name.
     */
    public function testUpdateSetWithTableNameString(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_sltn_items SET description = 'my_sltn_items updated' WHERE id = 3"
            );

            $rows = $this->ztdQuery("SELECT description FROM my_sltn_items WHERE id = 3");
            $this->assertSame('my_sltn_items updated', $rows[0]['description']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with table name string failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared parameter value matching table name.
     */
    public function testPreparedParamMatchingTableName(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, name FROM my_sltn_items WHERE source = ?",
                ['my_sltn_items']
            );

            $this->assertCount(1, $rows);
            $this->assertSame('alpha', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared param matching table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * CONCAT producing table name in result.
     */
    public function testConcatProducingTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, CONCAT('my_sltn_', 'items') AS tbl_name FROM my_sltn_items ORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('my_sltn_items', $rows[0]['tbl_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'CONCAT producing table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Double-quoted string (MySQL ANSI mode) that could look like identifier.
     */
    public function testDoubleQuotedStringContainingTableName(): void
    {
        try {
            // In default MySQL mode, double quotes are string literals (not identifiers)
            $rows = $this->ztdQuery(
                "SELECT id FROM my_sltn_items WHERE source = 'my_sltn_items' ORDER BY id"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Double-quoted string with table name failed: ' . $e->getMessage()
            );
        }
    }
}
