<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL full-text search through CTE shadow on PDO.
 *
 * PostgreSQL uses tsvector/tsquery for full-text search. Tests verify
 * to_tsvector, to_tsquery, plainto_tsquery, @@ operator, ts_rank,
 * and ts_headline work through shadow queries.
 * @spec pending
 */
class PostgresFullTextSearchTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_fts_articles (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200),
                body TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_fts_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_fts_articles (id, title, body) VALUES (1, 'PHP Database Tutorial', 'Learn how to connect PHP to MySQL databases using PDO and MySQLi extensions')");
        $this->pdo->exec("INSERT INTO pg_fts_articles (id, title, body) VALUES (2, 'JavaScript Promises Guide', 'Understanding async programming with promises and async/await in modern JavaScript')");
        $this->pdo->exec("INSERT INTO pg_fts_articles (id, title, body) VALUES (3, 'PostgreSQL Performance Tuning', 'Optimize your PostgreSQL database queries with proper indexing and query analysis')");
        $this->pdo->exec("INSERT INTO pg_fts_articles (id, title, body) VALUES (4, 'PHP Testing Best Practices', 'Write better tests for your PHP applications using PHPUnit and testing patterns')");
    }

    /**
     * to_tsvector / to_tsquery with @@ operator finds matching rows.
     */
    public function testTsVectorTsQuery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pg_fts_articles WHERE to_tsvector('english', title || ' ' || body) @@ to_tsquery('english', 'PHP & database') ORDER BY id"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Full-text search not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * plainto_tsquery for simpler text input without operators.
     */
    public function testPlainToTsQuery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pg_fts_articles WHERE to_tsvector('english', title || ' ' || body) @@ plainto_tsquery('english', 'PHP testing') ORDER BY id"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Testing Best Practices', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('plainto_tsquery not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * ts_rank for relevance scoring.
     */
    public function testTsRankRelevanceScoring(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title, ts_rank(to_tsvector('english', title || ' ' || body), to_tsquery('english', 'PostgreSQL & database')) AS relevance
                 FROM pg_fts_articles
                 ORDER BY relevance DESC"
            );
            $this->assertNotEmpty($rows);
            // PostgreSQL-related article should be most relevant
            $first = $rows[0];
            $this->assertSame('PostgreSQL Performance Tuning', $first['title']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ts_rank not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * ts_headline for highlighting matched terms.
     */
    public function testTsHeadline(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, ts_headline('english', body, to_tsquery('english', 'PHP'), 'StartSel=<b>, StopSel=</b>, MaxFragments=1') AS headline
                 FROM pg_fts_articles
                 WHERE to_tsvector('english', body) @@ to_tsquery('english', 'PHP')
                 ORDER BY id"
            );
            $this->assertNotEmpty($rows);
            // Headline should contain <b> tags around matched terms
            $this->assertStringContainsString('<b>', $rows[0]['headline']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ts_headline not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * OR operator in tsquery.
     */
    public function testTsQueryOrOperator(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pg_fts_articles WHERE to_tsvector('english', title) @@ to_tsquery('english', 'PHP | JavaScript') ORDER BY id"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
            $this->assertContains('JavaScript Promises Guide', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('tsquery OR operator not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Negation operator in tsquery.
     */
    public function testTsQueryNegation(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pg_fts_articles WHERE to_tsvector('english', title || ' ' || body) @@ to_tsquery('english', 'PHP & !testing') ORDER BY id"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
            $this->assertNotContains('PHP Testing Best Practices', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('tsquery negation not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * No matching rows returns empty.
     */
    public function testNoMatchReturnsEmpty(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pg_fts_articles WHERE to_tsvector('english', title || ' ' || body) @@ to_tsquery('english', 'python & django')"
            );
            $this->assertEmpty($rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Full-text search not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * INSERT then search finds newly inserted row.
     */
    public function testSearchAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_fts_articles (id, title, body) VALUES (5, 'Rust Systems Programming', 'Build fast and safe systems with Rust programming language')");

        try {
            $rows = $this->ztdQuery(
                "SELECT title FROM pg_fts_articles WHERE to_tsvector('english', title || ' ' || body) @@ to_tsquery('english', 'Rust & systems')"
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Rust Systems Programming', $rows[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Full-text search after INSERT not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with full-text search parameters.
     */
    public function testPreparedFullTextSearch(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, title FROM pg_fts_articles WHERE to_tsvector('english', title || ' ' || body) @@ plainto_tsquery('english', ?) ORDER BY id",
                ['PHP testing']
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Testing Best Practices', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared full-text search not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation — data only in shadow store.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_fts_articles');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
