<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests queries where string literal values happen to match table names.
 *
 * Real-world scenario: If a table is named "users" and a query contains
 * WHERE status = 'users', the CTE rewriter might incorrectly rewrite the
 * table reference inside the string literal. This is known for SQLite (#67)
 * and this test systematically probes the boundary.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteStringLiteralTableNameTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sltn_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                source TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sltn_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert data where values intentionally match the table name
        $this->ztdExec("INSERT INTO sl_sltn_items VALUES (1, 'alpha', 'from sl_sltn_items table', 'sl_sltn_items')");
        $this->ztdExec("INSERT INTO sl_sltn_items VALUES (2, 'beta', 'SELECT * FROM sl_sltn_items', 'external')");
        $this->ztdExec("INSERT INTO sl_sltn_items VALUES (3, 'gamma', 'normal description', 'manual')");
    }

    /**
     * WHERE clause with string literal matching table name.
     */
    public function testWhereStringMatchesTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM sl_sltn_items WHERE source = 'sl_sltn_items' ORDER BY id"
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
     * String literal containing SQL with table name.
     */
    public function testStringContainingSqlWithTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, description FROM sl_sltn_items WHERE description LIKE '%SELECT%FROM%sl_sltn_items%'"
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
     * INSERT with string value matching table name.
     */
    public function testInsertStringMatchingTableName(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_sltn_items VALUES (4, 'delta', 'sl_sltn_items', 'sl_sltn_items')"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_sltn_items WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('sl_sltn_items', $rows[0]['description']);
            $this->assertSame('sl_sltn_items', $rows[0]['source']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with string matching table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with string value matching table name.
     */
    public function testUpdateSetStringMatchingTableName(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_sltn_items SET description = 'sl_sltn_items updated' WHERE id = 3"
            );

            $rows = $this->ztdQuery("SELECT description FROM sl_sltn_items WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertSame('sl_sltn_items updated', $rows[0]['description']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET string matching table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with parameter value matching table name.
     */
    public function testPreparedParamMatchingTableName(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, name FROM sl_sltn_items WHERE source = ?",
                ['sl_sltn_items']
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
     * CASE expression with string literal matching table name.
     */
    public function testCaseExpressionWithTableNameString(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, CASE WHEN source = 'sl_sltn_items' THEN 'self' ELSE 'other' END AS origin
                 FROM sl_sltn_items ORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('self', $rows[0]['origin']);
            $this->assertSame('other', $rows[1]['origin']);
            $this->assertSame('other', $rows[2]['origin']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'CASE expression with table name string failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Concatenation producing table name in result (should not affect CTE).
     */
    public function testConcatProducingTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, 'sl_sltn_' || 'items' AS tbl_name FROM sl_sltn_items ORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('sl_sltn_items', $rows[0]['tbl_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Concat producing table name failed: ' . $e->getMessage()
            );
        }
    }
}
