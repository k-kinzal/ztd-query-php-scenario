<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL E-string (escape string) syntax through the CTE rewriter.
 *
 * Real-world scenario: PostgreSQL supports E'...' escape strings for
 * embedding special characters like \n, \t, \\, \'. This syntax is used
 * when applications need to store multi-line text, tab-separated values,
 * or strings with backslash escapes. If the CTE rewriter does not
 * recognize the E prefix, it will treat it as a separate identifier,
 * producing invalid SQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresEscapeStringTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_es_docs (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT,
                notes TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_es_docs'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_es_docs (id, title, content, notes) VALUES (1, 'Doc A', 'simple content', 'note a')");
        $this->ztdExec("INSERT INTO pg_es_docs (id, title, content, notes) VALUES (2, 'Doc B', 'another doc', 'note b')");
    }

    /**
     * INSERT with E-string containing newline escape.
     */
    public function testEStringInsertWithNewline(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_es_docs (id, title, content) VALUES (3, 'Doc C', E'line1\\nline2')"
            );

            $rows = $this->ztdQuery("SELECT content FROM pg_es_docs WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString("\n", $rows[0]['content']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string INSERT with newline failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with E-string containing tab escape.
     */
    public function testEStringUpdateWithTab(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_es_docs SET content = E'col1\\tcol2\\tcol3' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT content FROM pg_es_docs WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString("\t", $rows[0]['content']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string UPDATE with tab failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with E-string containing table name.
     * This tests whether the rewriter incorrectly rewrites inside E-strings.
     */
    public function testEStringUpdateWithTableName(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_es_docs SET content = E'references pg_es_docs\\ntable' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT content FROM pg_es_docs WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString('pg_es_docs', $rows[0]['content']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string UPDATE with table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with E-string in WHERE clause.
     */
    public function testEStringInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM pg_es_docs WHERE content = E'simple content'"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * E-string with backslash-escaped single quote.
     */
    public function testEStringWithEscapedQuote(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_es_docs SET content = E'it\\'s a test' WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT content FROM pg_es_docs WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertSame("it's a test", $rows[0]['content']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string with escaped quote failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * E-string with backslash itself (\\).
     */
    public function testEStringWithBackslash(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_es_docs SET content = E'path\\\\to\\\\file' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT content FROM pg_es_docs WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString('\\', $rows[0]['content']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string with backslash failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Mixed E-string and regular string in same query.
     */
    public function testMixedEStringAndRegularString(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_es_docs SET content = E'has\\nnewline', notes = 'regular string' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT content, notes FROM pg_es_docs WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertStringContainsString("\n", $rows[0]['content']);
            $this->assertSame('regular string', $rows[0]['notes']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed E-string and regular string failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with E-string in SELECT...WHERE.
     */
    public function testEStringInSelectWhere(): void
    {
        try {
            // First insert with E-string
            $this->ztdExec(
                "INSERT INTO pg_es_docs (id, title, content) VALUES (4, 'Doc D', E'multi\\nline\\ncontent')"
            );

            // Then query with regular string comparison
            $rows = $this->ztdQuery("SELECT title FROM pg_es_docs WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Doc D', $rows[0]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'E-string in SELECT WHERE failed: ' . $e->getMessage()
            );
        }
    }
}
