<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a content versioning (draft/publish) workflow through ZTD shadow store (MySQLi).
 * Covers latest-version subquery, status transitions, version comparison,
 * rollback, cross-table aggregation, and physical isolation.
 * @spec SPEC-10.2.60
 */
class ContentVersioningTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cv_articles (
                id INT PRIMARY KEY,
                title VARCHAR(255),
                author VARCHAR(255),
                status VARCHAR(20),
                created_at DATETIME
            )',
            'CREATE TABLE mi_cv_versions (
                id INT PRIMARY KEY,
                article_id INT,
                version_num INT,
                content TEXT,
                change_note VARCHAR(255),
                created_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cv_versions', 'mi_cv_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cv_articles VALUES (1, 'Getting Started with PHP', 'Alice', 'published', '2025-01-01 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_cv_articles VALUES (2, 'Advanced SQL Patterns', 'Bob', 'draft', '2025-02-15 14:00:00')");
        $this->mysqli->query("INSERT INTO mi_cv_articles VALUES (3, 'Testing Best Practices', 'Charlie', 'archived', '2024-06-01 08:00:00')");

        $this->mysqli->query("INSERT INTO mi_cv_versions VALUES (1, 1, 1, 'PHP is a popular language.', 'Initial draft', '2025-01-01 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_cv_versions VALUES (2, 1, 2, 'PHP is a popular server-side language used for web development.', 'Expanded intro', '2025-01-05 14:00:00')");
        $this->mysqli->query("INSERT INTO mi_cv_versions VALUES (3, 1, 3, 'PHP is a widely-used server-side scripting language designed for web development. It powers over 75% of websites.', 'Added statistics', '2025-01-10 09:00:00')");

        $this->mysqli->query("INSERT INTO mi_cv_versions VALUES (4, 2, 1, 'SQL is essential for data work.', 'First draft', '2025-02-15 14:00:00')");
        $this->mysqli->query("INSERT INTO mi_cv_versions VALUES (5, 2, 2, 'SQL is essential for data work. This article covers CTEs, window functions, and subqueries.', 'Added outline', '2025-02-20 11:00:00')");

        $this->mysqli->query("INSERT INTO mi_cv_versions VALUES (6, 3, 1, 'Testing is important for code quality.', 'Only version', '2024-06-01 08:00:00')");
    }

    public function testLatestVersionPerArticle(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, v.version_num, v.content
             FROM mi_cv_articles a
             JOIN mi_cv_versions v ON v.article_id = a.id
             WHERE v.version_num = (
                 SELECT MAX(v2.version_num)
                 FROM mi_cv_versions v2
                 WHERE v2.article_id = a.id
             )
             ORDER BY a.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['version_num']);
        $this->assertStringContainsString('75% of websites', $rows[0]['content']);
    }

    public function testFilterByStatus(): void
    {
        $rows = $this->ztdQuery("SELECT title FROM mi_cv_articles WHERE status = 'published'");
        $this->assertCount(1, $rows);

        $rows = $this->ztdQuery("SELECT title FROM mi_cv_articles WHERE status = 'draft'");
        $this->assertCount(1, $rows);
    }

    public function testPublishArticle(): void
    {
        $this->mysqli->query("UPDATE mi_cv_articles SET status = 'published' WHERE id = 2 AND status = 'draft'");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery("SELECT status FROM mi_cv_articles WHERE id = 2");
        $this->assertSame('published', $rows[0]['status']);

        $this->mysqli->query("UPDATE mi_cv_articles SET status = 'published' WHERE id = 2 AND status = 'draft'");
        $this->assertSame(0, $this->mysqli->lastAffectedRows());
    }

    public function testCreateNewVersion(): void
    {
        $rows = $this->ztdQuery("SELECT MAX(version_num) AS latest FROM mi_cv_versions WHERE article_id = 2");
        $this->assertEquals(2, (int) $rows[0]['latest']);

        $this->mysqli->query(
            "INSERT INTO mi_cv_versions VALUES (7, 2, 3, 'SQL is essential for data work. This article covers CTEs, window functions, subqueries, and set operations like UNION and EXCEPT.', 'Added set operations', '2025-03-01 10:00:00')"
        );

        $rows = $this->ztdQuery("SELECT MAX(version_num) AS latest FROM mi_cv_versions WHERE article_id = 2");
        $this->assertEquals(3, (int) $rows[0]['latest']);
    }

    public function testVersionComparison(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.version_num, v.content
             FROM mi_cv_versions v
             WHERE v.article_id = 1 AND v.version_num IN (2, 3)
             ORDER BY v.version_num"
        );

        $this->assertCount(2, $rows);
        $this->assertStringContainsString('server-side language', $rows[0]['content']);
        $this->assertStringContainsString('75% of websites', $rows[1]['content']);
    }

    public function testRollbackToPreviousVersion(): void
    {
        $rows = $this->ztdQuery("SELECT content FROM mi_cv_versions WHERE article_id = 1 AND version_num = 1");
        $this->assertSame('PHP is a popular language.', $rows[0]['content']);

        $this->mysqli->query(
            "INSERT INTO mi_cv_versions VALUES (7, 1, 4, 'PHP is a popular language.', 'Rollback to v1', '2025-02-01 10:00:00')"
        );

        $rows = $this->ztdQuery(
            "SELECT v.version_num, v.content, v.change_note
             FROM mi_cv_versions v
             WHERE v.article_id = 1
               AND v.version_num = (SELECT MAX(v2.version_num) FROM mi_cv_versions v2 WHERE v2.article_id = 1)"
        );
        $this->assertEquals(4, (int) $rows[0]['version_num']);
        $this->assertSame('PHP is a popular language.', $rows[0]['content']);
    }

    public function testArticleSummaryReport(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, a.status,
                    COUNT(v.id) AS version_count,
                    MAX(v.version_num) AS latest_version
             FROM mi_cv_articles a
             JOIN mi_cv_versions v ON v.article_id = a.id
             GROUP BY a.id, a.title, a.status
             ORDER BY version_count DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(3, (int) $rows[0]['version_count']);
        $this->assertEquals(2, (int) $rows[1]['version_count']);
        $this->assertEquals(1, (int) $rows[2]['version_count']);
    }

    public function testPreparedArticleLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT a.title, a.status, v.version_num
             FROM mi_cv_articles a
             JOIN mi_cv_versions v ON v.article_id = a.id
             WHERE a.author = ?
               AND v.version_num = (SELECT MAX(v2.version_num) FROM mi_cv_versions v2 WHERE v2.article_id = a.id)",
            ['Alice']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['version_num']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_cv_articles VALUES (4, 'New Article', 'Diana', 'draft', '2026-03-09 12:00:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cv_articles");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cv_articles');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
