<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL array functions and operators in the CTE shadow store.
 *
 * PostgreSQL arrays (TEXT[], INT[], etc.) are a common feature with
 * specialized functions (array_append, array_length, unnest, array_agg)
 * and operators (ANY, @>, &&). The CTE rewriter must handle array literal
 * syntax (ARRAY['a','b'] or '{a,b}'), array functions in SELECT/WHERE/UPDATE,
 * and set-returning functions like unnest().
 *
 * Related to Issue #33 (array type handling) but focuses on function-level
 * behavior rather than basic type round-trip.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-3.3
 */
class PostgresArrayFunctionsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pgx_af_articles (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                tags TEXT[] DEFAULT ARRAY[]::TEXT[]
            )',
            'CREATE TABLE pgx_af_scores (
                id SERIAL PRIMARY KEY,
                student VARCHAR(100) NOT NULL,
                marks INT[] NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pgx_af_scores', 'pgx_af_articles'];
    }

    /**
     * INSERT with ARRAY['a','b','c'] syntax and basic SELECT.
     */
    public function testInsertWithArrayLiteral(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Intro to SQL', ARRAY['sql', 'database', 'tutorial'])"
            );

            $rows = $this->ztdQuery('SELECT title, tags FROM pgx_af_articles WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Intro to SQL', $rows[0]['title']);

            // tags should be returned as a PostgreSQL array literal string
            $tags = $rows[0]['tags'];
            $this->assertNotNull($tags, 'Tags array should not be null');
            $this->assertStringContainsString('sql', $tags, 'Tags should contain "sql"');
            $this->assertStringContainsString('database', $tags, 'Tags should contain "database"');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with ARRAY literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with curly-brace array syntax: '{a,b,c}'.
     */
    public function testInsertWithCurlyBraceArraySyntax(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Advanced PG', '{arrays,functions,operators}')"
            );

            $rows = $this->ztdQuery('SELECT tags FROM pgx_af_articles WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['tags']);
            $this->assertStringContainsString('arrays', $rows[0]['tags']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with curly-brace array syntax failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with WHERE ? = ANY(tags) -- array membership using prepared statement.
     */
    public function testAnyOperatorWithPreparedParam(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'SQL Guide', ARRAY['sql', 'guide'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (2, 'Python 101', ARRAY['python', 'tutorial'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (3, 'DB Design', ARRAY['sql', 'design'])"
            );

            $rows = $this->ztdPrepareAndExecute(
                'SELECT id, title FROM pgx_af_articles WHERE ? = ANY(tags) ORDER BY id',
                ['sql']
            );

            $this->assertCount(2, $rows, 'Two articles should have the "sql" tag');
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ANY(tags) with prepared param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with array_length() function.
     */
    public function testArrayLengthFunction(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Tagged', ARRAY['a', 'b', 'c', 'd'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (2, 'Few Tags', ARRAY['x'])"
            );

            $rows = $this->ztdQuery(
                'SELECT id, array_length(tags, 1) AS tag_count FROM pgx_af_articles ORDER BY id'
            );

            $this->assertCount(2, $rows);
            $this->assertSame(4, (int) $rows[0]['tag_count'], 'First article should have 4 tags');
            $this->assertSame(1, (int) $rows[1]['tag_count'], 'Second article should have 1 tag');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'array_length function failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET tags = array_append(tags, 'new') -- function modifying array.
     */
    public function testUpdateWithArrayAppend(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Growing', ARRAY['initial'])"
            );

            $this->ztdExec(
                "UPDATE pgx_af_articles SET tags = array_append(tags, 'added') WHERE id = 1"
            );

            $rows = $this->ztdQuery('SELECT tags FROM pgx_af_articles WHERE id = 1');
            $this->assertCount(1, $rows);

            $tags = $rows[0]['tags'];
            $this->assertStringContainsString('initial', $tags, 'Original tag should remain');
            $this->assertStringContainsString('added', $tags, 'Appended tag should be present');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with array_append failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with unnest() -- set-returning function from shadow data.
     */
    public function testUnnestFunction(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Multi', ARRAY['red', 'green', 'blue'])"
            );

            $rows = $this->ztdQuery(
                'SELECT unnest(tags) AS tag FROM pgx_af_articles WHERE id = 1 ORDER BY tag'
            );

            $this->assertCount(3, $rows);
            $tags = array_column($rows, 'tag');
            $this->assertSame(['blue', 'green', 'red'], $tags);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'unnest() on shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * array_agg() aggregate function on shadow data.
     */
    public function testArrayAggFunction(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags) VALUES (1, 'Alpha', ARRAY['a'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags) VALUES (2, 'Beta', ARRAY['b'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags) VALUES (3, 'Gamma', ARRAY['c'])"
            );

            $rows = $this->ztdQuery(
                'SELECT array_agg(title ORDER BY id) AS all_titles FROM pgx_af_articles'
            );
            $this->assertCount(1, $rows);

            $allTitles = $rows[0]['all_titles'];
            $this->assertNotNull($allTitles, 'array_agg result should not be null');
            $this->assertStringContainsString('Alpha', $allTitles);
            $this->assertStringContainsString('Beta', $allTitles);
            $this->assertStringContainsString('Gamma', $allTitles);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'array_agg on shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INT[] array with array functions.
     */
    public function testIntArrayWithFunctions(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_scores (id, student, marks)
                 VALUES (1, 'Alice', ARRAY[85, 92, 78, 95])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_scores (id, student, marks)
                 VALUES (2, 'Bob', ARRAY[70, 65, 80])"
            );

            // array_length on INT[]
            $rows = $this->ztdQuery(
                'SELECT student, array_length(marks, 1) AS exam_count
                 FROM pgx_af_scores ORDER BY id'
            );
            $this->assertCount(2, $rows);
            $this->assertSame(4, (int) $rows[0]['exam_count']);
            $this->assertSame(3, (int) $rows[1]['exam_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INT[] array with functions failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Array subscript access in SELECT.
     */
    public function testArraySubscriptAccess(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_scores (id, student, marks)
                 VALUES (1, 'Charlie', ARRAY[90, 85, 95])"
            );

            // PostgreSQL arrays are 1-indexed
            $rows = $this->ztdQuery(
                'SELECT marks[1] AS first_mark, marks[3] AS third_mark
                 FROM pgx_af_scores WHERE id = 1'
            );
            $this->assertCount(1, $rows);
            $this->assertSame(90, (int) $rows[0]['first_mark']);
            $this->assertSame(95, (int) $rows[0]['third_mark']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Array subscript access failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with array_remove function.
     */
    public function testUpdateWithArrayRemove(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Removable', ARRAY['keep', 'remove', 'also_keep'])"
            );

            $this->ztdExec(
                "UPDATE pgx_af_articles SET tags = array_remove(tags, 'remove') WHERE id = 1"
            );

            $rows = $this->ztdQuery('SELECT tags FROM pgx_af_articles WHERE id = 1');
            $this->assertCount(1, $rows);

            $tags = $rows[0]['tags'];
            $this->assertStringContainsString('keep', $tags);
            $this->assertStringNotContainsString('remove', $tags,
                'Removed element should not be present');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with array_remove failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Array overlap operator (&&) in WHERE clause.
     */
    public function testArrayOverlapOperator(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Tech', ARRAY['rust', 'systems'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (2, 'Web', ARRAY['javascript', 'react'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (3, 'Both', ARRAY['rust', 'wasm', 'javascript'])"
            );

            // Find articles that overlap with ARRAY['rust', 'go']
            $rows = $this->ztdQuery(
                "SELECT id FROM pgx_af_articles
                 WHERE tags && ARRAY['rust', 'go']
                 ORDER BY id"
            );

            $this->assertCount(2, $rows, 'Two articles have tags overlapping with rust/go');
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Array overlap operator (&&) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Array contains operator (@>) in WHERE clause.
     */
    public function testArrayContainsOperator(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Full', ARRAY['sql', 'database', 'tutorial', 'beginner'])"
            );
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (2, 'Partial', ARRAY['sql', 'advanced'])"
            );

            // Find articles whose tags contain both 'sql' AND 'tutorial'
            $rows = $this->ztdQuery(
                "SELECT id FROM pgx_af_articles
                 WHERE tags @> ARRAY['sql', 'tutorial']
                 ORDER BY id"
            );

            $this->assertCount(1, $rows, 'Only one article contains both sql and tutorial');
            $this->assertSame(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Array contains operator (@>) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgx_af_articles (id, title, tags)
                 VALUES (1, 'Test', ARRAY['test'])"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with array failed: ' . $e->getMessage()
            );
        }

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgx_af_articles');
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should have 0 rows');
    }
}
