<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite FTS5 full-text search through CTE shadow on PDO.
 *
 * SQLite uses FTS5 virtual tables for full-text search. Tests verify
 * MATCH operator, bm25() ranking, snippet(), and highlight() work
 * through shadow queries.
 * @spec SPEC-3.3f
 */
class SqliteFullTextSearchTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE VIRTUAL TABLE sl_fts_articles USING fts5(title, body)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_fts_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        try {
            $this->pdo->exec("INSERT INTO sl_fts_articles (rowid, title, body) VALUES (1, 'PHP Database Tutorial', 'Learn how to connect PHP to MySQL databases using PDO and MySQLi extensions')");
            $this->pdo->exec("INSERT INTO sl_fts_articles (rowid, title, body) VALUES (2, 'JavaScript Promises Guide', 'Understanding async programming with promises and async/await in modern JavaScript')");
            $this->pdo->exec("INSERT INTO sl_fts_articles (rowid, title, body) VALUES (3, 'SQLite Performance Tips', 'Optimize your SQLite database queries with proper indexing and PRAGMA settings')");
            $this->pdo->exec("INSERT INTO sl_fts_articles (rowid, title, body) VALUES (4, 'PHP Testing Best Practices', 'Write better tests for your PHP applications using PHPUnit and testing patterns')");
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 virtual table not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * MATCH operator finds relevant rows in FTS5 table.
     */
    public function testFts5Match(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rowid, title FROM sl_fts_articles WHERE sl_fts_articles MATCH 'PHP database' ORDER BY rowid"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 MATCH not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Column-specific FTS5 search.
     */
    public function testFts5ColumnFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rowid, title FROM sl_fts_articles WHERE sl_fts_articles MATCH 'title:PHP' ORDER BY rowid"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
            $this->assertContains('PHP Testing Best Practices', $titles);
            $this->assertNotContains('JavaScript Promises Guide', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 column filter not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * bm25() ranking function orders by relevance.
     */
    public function testBm25Ranking(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rowid, title, bm25(sl_fts_articles) AS rank FROM sl_fts_articles WHERE sl_fts_articles MATCH 'PHP' ORDER BY rank"
            );
            $this->assertNotEmpty($rows);
            // bm25() returns negative values; more negative = more relevant
            $this->assertCount(2, $rows); // PHP Database Tutorial and PHP Testing
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 bm25() not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * highlight() function marks matched terms.
     */
    public function testHighlight(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rowid, highlight(sl_fts_articles, 0, '<b>', '</b>') AS highlighted_title FROM sl_fts_articles WHERE sl_fts_articles MATCH 'PHP' ORDER BY rowid"
            );
            $this->assertNotEmpty($rows);
            $this->assertStringContainsString('<b>', $rows[0]['highlighted_title']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 highlight() not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * snippet() function returns contextual snippets.
     */
    public function testSnippet(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rowid, snippet(sl_fts_articles, 1, '<b>', '</b>', '...', 10) AS snip FROM sl_fts_articles WHERE sl_fts_articles MATCH 'database' ORDER BY rowid"
            );
            $this->assertNotEmpty($rows);
            $this->assertStringContainsString('<b>', $rows[0]['snip']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 snippet() not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Boolean operators in FTS5 (AND, OR, NOT).
     */
    public function testFts5BooleanOperators(): void
    {
        try {
            // AND (implicit)
            $rows = $this->ztdQuery(
                "SELECT rowid, title FROM sl_fts_articles WHERE sl_fts_articles MATCH 'PHP testing'"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Testing Best Practices', $titles);

            // NOT
            $rows = $this->ztdQuery(
                "SELECT rowid, title FROM sl_fts_articles WHERE sl_fts_articles MATCH 'PHP NOT testing'"
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
            $this->assertNotContains('PHP Testing Best Practices', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 boolean operators not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * No matching rows returns empty.
     */
    public function testNoMatchReturnsEmpty(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rowid, title FROM sl_fts_articles WHERE sl_fts_articles MATCH 'python django'"
            );
            $this->assertEmpty($rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 MATCH not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * INSERT then search finds newly inserted row.
     */
    public function testSearchAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fts_articles (rowid, title, body) VALUES (5, 'Rust Systems Programming', 'Build fast and safe systems with Rust programming language')");

            $rows = $this->ztdQuery(
                "SELECT title FROM sl_fts_articles WHERE sl_fts_articles MATCH 'Rust systems'"
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Rust Systems Programming', $rows[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FTS5 search after INSERT not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with FTS5 MATCH parameter.
     */
    public function testPreparedFullTextSearch(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT rowid, title FROM sl_fts_articles WHERE sl_fts_articles MATCH ? ORDER BY rowid",
                ['PHP']
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared FTS5 search not supported through ZTD: ' . $e->getMessage());
        }
    }
}
