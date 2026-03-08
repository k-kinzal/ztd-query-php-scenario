<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a content versioning (draft/publish) workflow through ZTD shadow store (SQLite PDO).
 * Covers latest-version subquery, status transitions, version comparison,
 * rollback, cross-table aggregation, and physical isolation.
 * @spec SPEC-10.2.60
 */
class SqliteContentVersioningTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cv_articles (
                id INTEGER PRIMARY KEY,
                title TEXT,
                author TEXT,
                status TEXT,
                created_at TEXT
            )',
            'CREATE TABLE sl_cv_versions (
                id INTEGER PRIMARY KEY,
                article_id INTEGER,
                version_num INTEGER,
                content TEXT,
                change_note TEXT,
                created_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cv_versions', 'sl_cv_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 articles
        $this->pdo->exec("INSERT INTO sl_cv_articles VALUES (1, 'Getting Started with PHP', 'Alice', 'published', '2025-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_cv_articles VALUES (2, 'Advanced SQL Patterns', 'Bob', 'draft', '2025-02-15 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_cv_articles VALUES (3, 'Testing Best Practices', 'Charlie', 'archived', '2024-06-01 08:00:00')");

        // Article 1: 3 versions (published at v3)
        $this->pdo->exec("INSERT INTO sl_cv_versions VALUES (1, 1, 1, 'PHP is a popular language.', 'Initial draft', '2025-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_cv_versions VALUES (2, 1, 2, 'PHP is a popular server-side language used for web development.', 'Expanded intro', '2025-01-05 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_cv_versions VALUES (3, 1, 3, 'PHP is a widely-used server-side scripting language designed for web development. It powers over 75% of websites.', 'Added statistics', '2025-01-10 09:00:00')");

        // Article 2: 2 versions (still draft)
        $this->pdo->exec("INSERT INTO sl_cv_versions VALUES (4, 2, 1, 'SQL is essential for data work.', 'First draft', '2025-02-15 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_cv_versions VALUES (5, 2, 2, 'SQL is essential for data work. This article covers CTEs, window functions, and subqueries.', 'Added outline', '2025-02-20 11:00:00')");

        // Article 3: 1 version (archived)
        $this->pdo->exec("INSERT INTO sl_cv_versions VALUES (6, 3, 1, 'Testing is important for code quality.', 'Only version', '2024-06-01 08:00:00')");
    }

    /**
     * Fetch latest version of each article using MAX subquery.
     */
    public function testLatestVersionPerArticle(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, v.version_num, v.content, v.change_note
             FROM sl_cv_articles a
             JOIN sl_cv_versions v ON v.article_id = a.id
             WHERE v.version_num = (
                 SELECT MAX(v2.version_num)
                 FROM sl_cv_versions v2
                 WHERE v2.article_id = a.id
             )
             ORDER BY a.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['version_num']);
        $this->assertStringContains('75% of websites', $rows[0]['content']);

        $this->assertSame('Advanced SQL Patterns', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['version_num']);

        $this->assertSame('Testing Best Practices', $rows[2]['title']);
        $this->assertEquals(1, (int) $rows[2]['version_num']);
    }

    /**
     * Filter articles by status.
     */
    public function testFilterByStatus(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title, status FROM sl_cv_articles WHERE status = 'published'"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);

        $rows = $this->ztdQuery(
            "SELECT title, status FROM sl_cv_articles WHERE status = 'draft'"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Advanced SQL Patterns', $rows[0]['title']);

        $rows = $this->ztdQuery(
            "SELECT title, status FROM sl_cv_articles WHERE status IN ('published', 'draft') ORDER BY title"
        );
        $this->assertCount(2, $rows);
    }

    /**
     * Publish an article: status transition from draft to published.
     */
    public function testPublishArticle(): void
    {
        $rows = $this->ztdQuery("SELECT status FROM sl_cv_articles WHERE id = 2");
        $this->assertSame('draft', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE sl_cv_articles SET status = 'published' WHERE id = 2 AND status = 'draft'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_cv_articles WHERE id = 2");
        $this->assertSame('published', $rows[0]['status']);

        // Cannot publish again (already published)
        $affected = $this->pdo->exec("UPDATE sl_cv_articles SET status = 'published' WHERE id = 2 AND status = 'draft'");
        $this->assertSame(0, $affected);
    }

    /**
     * Create a new version and verify it becomes the latest.
     */
    public function testCreateNewVersion(): void
    {
        // Current latest for article 2 is v2
        $rows = $this->ztdQuery(
            "SELECT MAX(version_num) AS latest FROM sl_cv_versions WHERE article_id = 2"
        );
        $this->assertEquals(2, (int) $rows[0]['latest']);

        // Add v3
        $this->pdo->exec(
            "INSERT INTO sl_cv_versions VALUES (7, 2, 3, 'SQL is essential for data work. This article covers CTEs, window functions, subqueries, and set operations like UNION and EXCEPT.', 'Added set operations', '2025-03-01 10:00:00')"
        );

        // Verify v3 is now latest
        $rows = $this->ztdQuery(
            "SELECT MAX(version_num) AS latest FROM sl_cv_versions WHERE article_id = 2"
        );
        $this->assertEquals(3, (int) $rows[0]['latest']);

        // Verify content via latest-version join
        $rows = $this->ztdQuery(
            "SELECT v.content FROM sl_cv_versions v
             WHERE v.article_id = 2
               AND v.version_num = (SELECT MAX(v2.version_num) FROM sl_cv_versions v2 WHERE v2.article_id = 2)"
        );
        $this->assertStringContains('set operations', $rows[0]['content']);
    }

    /**
     * Version comparison: current vs previous version content.
     */
    public function testVersionComparison(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.version_num, v.content, v.change_note
             FROM sl_cv_versions v
             WHERE v.article_id = 1 AND v.version_num IN (2, 3)
             ORDER BY v.version_num"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(2, (int) $rows[0]['version_num']);
        $this->assertEquals(3, (int) $rows[1]['version_num']);
        $this->assertStringContains('server-side language', $rows[0]['content']);
        $this->assertStringContains('75% of websites', $rows[1]['content']);
    }

    /**
     * Rollback: create new version from a previous version's content.
     */
    public function testRollbackToPreviousVersion(): void
    {
        // Get v1 content for article 1
        $rows = $this->ztdQuery(
            "SELECT content FROM sl_cv_versions WHERE article_id = 1 AND version_num = 1"
        );
        $v1Content = $rows[0]['content'];
        $this->assertSame('PHP is a popular language.', $v1Content);

        // Create v4 as rollback to v1
        $this->pdo->exec(
            "INSERT INTO sl_cv_versions VALUES (7, 1, 4, 'PHP is a popular language.', 'Rollback to v1', '2025-02-01 10:00:00')"
        );

        // Verify latest is v4 with v1 content
        $rows = $this->ztdQuery(
            "SELECT v.version_num, v.content, v.change_note
             FROM sl_cv_versions v
             WHERE v.article_id = 1
               AND v.version_num = (SELECT MAX(v2.version_num) FROM sl_cv_versions v2 WHERE v2.article_id = 1)"
        );
        $this->assertEquals(4, (int) $rows[0]['version_num']);
        $this->assertSame('PHP is a popular language.', $rows[0]['content']);
        $this->assertSame('Rollback to v1', $rows[0]['change_note']);
    }

    /**
     * Article summary: version count, latest content length, status.
     */
    public function testArticleSummaryReport(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, a.author, a.status,
                    COUNT(v.id) AS version_count,
                    MAX(v.version_num) AS latest_version
             FROM sl_cv_articles a
             JOIN sl_cv_versions v ON v.article_id = a.id
             GROUP BY a.id, a.title, a.author, a.status
             ORDER BY version_count DESC"
        );

        $this->assertCount(3, $rows);

        // Article 1: 3 versions
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['version_count']);
        $this->assertEquals(3, (int) $rows[0]['latest_version']);
        $this->assertSame('published', $rows[0]['status']);

        // Article 2: 2 versions
        $this->assertSame('Advanced SQL Patterns', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['version_count']);
        $this->assertSame('draft', $rows[1]['status']);

        // Article 3: 1 version
        $this->assertSame('Testing Best Practices', $rows[2]['title']);
        $this->assertEquals(1, (int) $rows[2]['version_count']);
        $this->assertSame('archived', $rows[2]['status']);
    }

    /**
     * Prepared statement: lookup article with version details by author.
     */
    public function testPreparedArticleLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT a.title, a.status, v.version_num, v.change_note
             FROM sl_cv_articles a
             JOIN sl_cv_versions v ON v.article_id = a.id
             WHERE a.author = ?
               AND v.version_num = (SELECT MAX(v2.version_num) FROM sl_cv_versions v2 WHERE v2.article_id = a.id)",
            ['Alice']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertSame('published', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['version_num']);
    }

    /**
     * Archive an article and verify it's excluded from published list.
     */
    public function testArchiveArticle(): void
    {
        $this->pdo->exec("UPDATE sl_cv_articles SET status = 'archived' WHERE id = 1 AND status = 'published'");

        $rows = $this->ztdQuery("SELECT title FROM sl_cv_articles WHERE status = 'published'");
        $this->assertCount(0, $rows);

        $rows = $this->ztdQuery("SELECT title FROM sl_cv_articles WHERE status = 'archived' ORDER BY title");
        $this->assertCount(2, $rows);
    }

    /**
     * Physical isolation verification.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_cv_articles VALUES (4, 'New Article', 'Diana', 'draft', '2026-03-09 12:00:00')");
        $this->pdo->exec("UPDATE sl_cv_articles SET status = 'archived' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_cv_articles");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_cv_articles")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }

    private function assertStringContains(string $needle, string $haystack): void
    {
        $this->assertTrue(
            str_contains($haystack, $needle),
            "Failed asserting that '{$haystack}' contains '{$needle}'"
        );
    }
}
