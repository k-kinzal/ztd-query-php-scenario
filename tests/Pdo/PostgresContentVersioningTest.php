<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a content versioning (draft/publish) workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers latest-version subquery, status transitions, version comparison,
 * rollback, cross-table aggregation, and physical isolation.
 * @spec SPEC-10.2.60
 */
class PostgresContentVersioningTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cv_articles (
                id INTEGER PRIMARY KEY,
                title VARCHAR(255),
                author VARCHAR(255),
                status VARCHAR(20),
                created_at TIMESTAMP
            )',
            'CREATE TABLE pg_cv_versions (
                id INTEGER PRIMARY KEY,
                article_id INTEGER,
                version_num INTEGER,
                content TEXT,
                change_note VARCHAR(255),
                created_at TIMESTAMP
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cv_versions', 'pg_cv_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cv_articles VALUES (1, 'Getting Started with PHP', 'Alice', 'published', '2025-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_cv_articles VALUES (2, 'Advanced SQL Patterns', 'Bob', 'draft', '2025-02-15 14:00:00')");
        $this->pdo->exec("INSERT INTO pg_cv_articles VALUES (3, 'Testing Best Practices', 'Charlie', 'archived', '2024-06-01 08:00:00')");

        $this->pdo->exec("INSERT INTO pg_cv_versions VALUES (1, 1, 1, 'PHP is a popular language.', 'Initial draft', '2025-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_cv_versions VALUES (2, 1, 2, 'PHP is a popular server-side language used for web development.', 'Expanded intro', '2025-01-05 14:00:00')");
        $this->pdo->exec("INSERT INTO pg_cv_versions VALUES (3, 1, 3, 'PHP is a widely-used server-side scripting language designed for web development. It powers over 75% of websites.', 'Added statistics', '2025-01-10 09:00:00')");

        $this->pdo->exec("INSERT INTO pg_cv_versions VALUES (4, 2, 1, 'SQL is essential for data work.', 'First draft', '2025-02-15 14:00:00')");
        $this->pdo->exec("INSERT INTO pg_cv_versions VALUES (5, 2, 2, 'SQL is essential for data work. This article covers CTEs, window functions, and subqueries.', 'Added outline', '2025-02-20 11:00:00')");

        $this->pdo->exec("INSERT INTO pg_cv_versions VALUES (6, 3, 1, 'Testing is important for code quality.', 'Only version', '2024-06-01 08:00:00')");
    }

    public function testLatestVersionPerArticle(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, v.version_num, v.content
             FROM pg_cv_articles a
             JOIN pg_cv_versions v ON v.article_id = a.id
             WHERE v.version_num = (
                 SELECT MAX(v2.version_num)
                 FROM pg_cv_versions v2
                 WHERE v2.article_id = a.id
             )
             ORDER BY a.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['version_num']);
    }

    public function testPublishArticle(): void
    {
        $affected = $this->pdo->exec("UPDATE pg_cv_articles SET status = 'published' WHERE id = 2 AND status = 'draft'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM pg_cv_articles WHERE id = 2");
        $this->assertSame('published', $rows[0]['status']);
    }

    public function testCreateNewVersion(): void
    {
        $this->pdo->exec(
            "INSERT INTO pg_cv_versions VALUES (7, 2, 3, 'SQL is essential for data work. Covers CTEs, window functions, subqueries, and set operations.', 'Added set operations', '2025-03-01 10:00:00')"
        );

        $rows = $this->ztdQuery("SELECT MAX(version_num) AS latest FROM pg_cv_versions WHERE article_id = 2");
        $this->assertEquals(3, (int) $rows[0]['latest']);
    }

    public function testRollbackToPreviousVersion(): void
    {
        $this->pdo->exec(
            "INSERT INTO pg_cv_versions VALUES (7, 1, 4, 'PHP is a popular language.', 'Rollback to v1', '2025-02-01 10:00:00')"
        );

        $rows = $this->ztdQuery(
            "SELECT v.version_num, v.content
             FROM pg_cv_versions v
             WHERE v.article_id = 1
               AND v.version_num = (SELECT MAX(v2.version_num) FROM pg_cv_versions v2 WHERE v2.article_id = 1)"
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
             FROM pg_cv_articles a
             JOIN pg_cv_versions v ON v.article_id = a.id
             GROUP BY a.id, a.title, a.status
             ORDER BY version_count DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(3, (int) $rows[0]['version_count']);
    }

    public function testPreparedArticleLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT a.title, v.version_num
             FROM pg_cv_articles a
             JOIN pg_cv_versions v ON v.article_id = a.id
             WHERE a.author = ?
               AND v.version_num = (SELECT MAX(v2.version_num) FROM pg_cv_versions v2 WHERE v2.article_id = a.id)",
            ['Alice']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['version_num']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_cv_articles VALUES (4, 'New Article', 'Diana', 'draft', '2026-03-09 12:00:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_cv_articles");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_cv_articles")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
