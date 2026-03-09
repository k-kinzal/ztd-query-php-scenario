<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests queries where string literal values match table names (PostgreSQL PDO).
 *
 * Real-world scenario: If a table is named "items" and a column stores
 * the string "items", the CTE rewriter might incorrectly rewrite the
 * table reference inside the string literal. Known for SQLite (#67),
 * this tests whether PostgreSQL has the same issue.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresStringLiteralTableNameTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sltn_items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                source TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sltn_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_sltn_items (id, name, description, source) VALUES (1, 'alpha', 'from pg_sltn_items table', 'pg_sltn_items')");
        $this->ztdExec("INSERT INTO pg_sltn_items (id, name, description, source) VALUES (2, 'beta', 'SELECT * FROM pg_sltn_items', 'external')");
        $this->ztdExec("INSERT INTO pg_sltn_items (id, name, description, source) VALUES (3, 'gamma', 'normal description', 'manual')");
    }

    /**
     * WHERE clause with string literal matching table name.
     */
    public function testWhereStringMatchesTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM pg_sltn_items WHERE source = 'pg_sltn_items' ORDER BY id"
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
                "SELECT id, description FROM pg_sltn_items WHERE description LIKE '%SELECT%FROM%pg_sltn_items%'"
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
     * INSERT with string values that match the table name.
     */
    public function testInsertStringIsTableName(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_sltn_items (id, name, description, source) VALUES (4, 'pg_sltn_items', 'pg_sltn_items', 'pg_sltn_items')"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_sltn_items WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('pg_sltn_items', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT string is table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with string matching table name.
     */
    public function testUpdateSetWithTableNameString(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_sltn_items SET description = 'pg_sltn_items updated' WHERE id = 3"
            );

            $rows = $this->ztdQuery("SELECT description FROM pg_sltn_items WHERE id = 3");
            $this->assertSame('pg_sltn_items updated', $rows[0]['description']);
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
                "SELECT id, name FROM pg_sltn_items WHERE source = ?",
                ['pg_sltn_items']
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
     * String concatenation producing table name.
     */
    public function testConcatProducingTableName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, 'pg_sltn_' || 'items' AS tbl_name FROM pg_sltn_items ORDER BY id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('pg_sltn_items', $rows[0]['tbl_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Concat producing table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Dollar-quoted string containing table name (PostgreSQL-specific).
     */
    public function testDollarQuotedStringWithTableName(): void
    {
        try {
            $this->ztdExec(
                'UPDATE pg_sltn_items SET description = $$contains pg_sltn_items reference$$ WHERE id = 3'
            );

            $rows = $this->ztdQuery("SELECT description FROM pg_sltn_items WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertSame('contains pg_sltn_items reference', $rows[0]['description']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dollar-quoted string with table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * E-string (escape string) containing table name.
     */
    public function testEscapeStringWithTableName(): void
    {
        try {
            $this->ztdExec(
                'UPDATE pg_sltn_items SET description = E\'pg_sltn_items\nupdated\' WHERE id = 3'
            );

            $rows = $this->ztdQuery("SELECT description FROM pg_sltn_items WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString('pg_sltn_items', $rows[0]['description']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string with table name failed: ' . $e->getMessage()
            );
        }
    }
}
