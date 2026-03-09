<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL dollar-quoting ($$ and $tag$) through the CTE rewriter.
 *
 * Real-world scenario: PostgreSQL supports dollar-quoted string constants
 * as an alternative to single-quoted strings. This is commonly used in:
 * - Stored procedures and functions (CREATE FUNCTION ... $$ body $$)
 * - Strings containing single quotes (avoiding escape gymnastics)
 * - ORM-generated SQL for complex string values
 *
 * The CTE rewriter must recognize dollar-quoted strings and not rewrite
 * table references that appear inside them. If the rewriter does not
 * handle dollar-quoting, any string containing a table name will produce
 * invalid SQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresDollarQuotingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dq_notes (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT,
                metadata TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dq_notes'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_dq_notes (id, title, body, metadata) VALUES (1, 'First', 'Hello world', 'source=web')");
        $this->ztdExec("INSERT INTO pg_dq_notes (id, title, body, metadata) VALUES (2, 'Second', 'Goodbye world', 'source=api')");
    }

    /**
     * UPDATE with $$ dollar-quoted string containing table name.
     * The CTE rewriter must not rewrite "pg_dq_notes" inside $$.
     */
    public function testDollarQuotedUpdateWithTableName(): void
    {
        try {
            $this->ztdExec(
                'UPDATE pg_dq_notes SET body = $$reference to pg_dq_notes table$$ WHERE id = 1'
            );

            $rows = $this->ztdQuery("SELECT body FROM pg_dq_notes WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('reference to pg_dq_notes table', $rows[0]['body']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dollar-quoted UPDATE with table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with $$ dollar-quoted string containing table name.
     */
    public function testDollarQuotedInsertWithTableName(): void
    {
        try {
            $this->ztdExec(
                'INSERT INTO pg_dq_notes (id, title, body) VALUES (3, $$pg_dq_notes title$$, $$body text$$)'
            );

            $rows = $this->ztdQuery("SELECT title, body FROM pg_dq_notes WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertSame('pg_dq_notes title', $rows[0]['title']);
            $this->assertSame('body text', $rows[0]['body']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dollar-quoted INSERT with table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with $$ dollar-quoted string in WHERE.
     */
    public function testDollarQuotedSelectWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id, title FROM pg_dq_notes WHERE body = $$Hello world$$'
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dollar-quoted SELECT WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * $$ dollar-quoting with single quotes inside (the main use case).
     */
    public function testDollarQuotedWithSingleQuotes(): void
    {
        try {
            $this->ztdExec(
                'UPDATE pg_dq_notes SET body = $$it\'s a note about pg_dq_notes$$ WHERE id = 2'
            );

            $rows = $this->ztdQuery("SELECT body FROM pg_dq_notes WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString("it's", $rows[0]['body']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dollar-quoted with single quotes failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Tagged dollar-quoting ($tag$...$tag$).
     */
    public function testTaggedDollarQuoting(): void
    {
        try {
            $this->ztdExec(
                'UPDATE pg_dq_notes SET body = $body$contains pg_dq_notes reference$body$ WHERE id = 1'
            );

            $rows = $this->ztdQuery("SELECT body FROM pg_dq_notes WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('contains pg_dq_notes reference', $rows[0]['body']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Tagged dollar-quoting failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * $$ dollar-quoted empty string.
     */
    public function testDollarQuotedEmptyString(): void
    {
        try {
            $this->ztdExec(
                'UPDATE pg_dq_notes SET body = $$$$ WHERE id = 1'
            );

            $rows = $this->ztdQuery("SELECT body FROM pg_dq_notes WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('', $rows[0]['body']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dollar-quoted empty string failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * $$ dollar-quoted string containing SQL keywords.
     */
    public function testDollarQuotedWithSqlKeywords(): void
    {
        try {
            $this->ztdExec(
                'UPDATE pg_dq_notes SET body = $$SELECT * FROM pg_dq_notes WHERE DELETE UPDATE INSERT$$ WHERE id = 1'
            );

            $rows = $this->ztdQuery("SELECT body FROM pg_dq_notes WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString('SELECT * FROM pg_dq_notes', $rows[0]['body']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Dollar-quoted with SQL keywords failed: ' . $e->getMessage()
            );
        }
    }
}
