<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests many-to-many relationship query patterns through ZTD shadow store.
 * Simulates blog/CMS tagging: posts have multiple tags, users filter by tags.
 * @spec SPEC-3.3
 */
class SqliteTagFilteringTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tf_posts (id INTEGER PRIMARY KEY, title TEXT, status TEXT, created_at TEXT)',
            'CREATE TABLE sl_tf_tags (id INTEGER PRIMARY KEY, name TEXT UNIQUE)',
            'CREATE TABLE sl_tf_post_tags (post_id INTEGER, tag_id INTEGER, PRIMARY KEY (post_id, tag_id))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tf_posts', 'sl_tf_tags', 'sl_tf_post_tags'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_tf_posts VALUES (1, 'Getting Started with PHP', 'published', '2024-01-10')");
        $this->pdo->exec("INSERT INTO sl_tf_posts VALUES (2, 'Advanced SQL Queries', 'published', '2024-02-15')");
        $this->pdo->exec("INSERT INTO sl_tf_posts VALUES (3, 'PHP and MySQL Guide', 'published', '2024-03-20')");
        $this->pdo->exec("INSERT INTO sl_tf_posts VALUES (4, 'JavaScript Basics', 'draft', '2024-04-05')");
        $this->pdo->exec("INSERT INTO sl_tf_posts VALUES (5, 'Database Design Tips', 'published', '2024-05-12')");

        $this->pdo->exec("INSERT INTO sl_tf_tags VALUES (1, 'php')");
        $this->pdo->exec("INSERT INTO sl_tf_tags VALUES (2, 'sql')");
        $this->pdo->exec("INSERT INTO sl_tf_tags VALUES (3, 'mysql')");
        $this->pdo->exec("INSERT INTO sl_tf_tags VALUES (4, 'javascript')");
        $this->pdo->exec("INSERT INTO sl_tf_tags VALUES (5, 'design')");

        // Post 1: php
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (1, 1)');
        // Post 2: sql
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (2, 2)');
        // Post 3: php, sql, mysql
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (3, 1)');
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (3, 2)');
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (3, 3)');
        // Post 4: javascript
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (4, 4)');
        // Post 5: sql, design
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (5, 2)');
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (5, 5)');
    }

    /**
     * Filter posts by a single tag using JOIN.
     */
    public function testFilterBySingleTag(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT p.title
             FROM sl_tf_posts p
             JOIN sl_tf_post_tags pt ON p.id = pt.post_id
             JOIN sl_tf_tags t ON pt.tag_id = t.id
             WHERE t.name = ? AND p.status = ?
             ORDER BY p.title'
        );
        $stmt->execute(['php', 'published']);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertCount(2, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]);
        $this->assertSame('PHP and MySQL Guide', $rows[1]);
    }

    /**
     * Filter posts that have ALL specified tags (intersection / "AND" logic).
     * Uses GROUP BY + HAVING COUNT = N pattern.
     */
    public function testFilterByAllTags(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.title
             FROM sl_tf_posts p
             JOIN sl_tf_post_tags pt ON p.id = pt.post_id
             JOIN sl_tf_tags t ON pt.tag_id = t.id
             WHERE t.name IN ('php', 'sql')
             GROUP BY p.id, p.title
             HAVING COUNT(DISTINCT t.id) = 2
             ORDER BY p.title"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('PHP and MySQL Guide', $rows[0]['title']);
    }

    /**
     * Filter posts that have ANY of the specified tags (union / "OR" logic).
     * Uses DISTINCT to avoid duplicates when a post matches multiple tags.
     */
    public function testFilterByAnyTag(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT p.title
             FROM sl_tf_posts p
             JOIN sl_tf_post_tags pt ON p.id = pt.post_id
             JOIN sl_tf_tags t ON pt.tag_id = t.id
             WHERE t.name IN ('php', 'javascript')
             ORDER BY p.title"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertSame('JavaScript Basics', $rows[1]['title']);
        $this->assertSame('PHP and MySQL Guide', $rows[2]['title']);
    }

    /**
     * Count tags per post (tag cloud / post detail).
     */
    public function testTagCountPerPost(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.title, COUNT(pt.tag_id) AS tag_count
             FROM sl_tf_posts p
             LEFT JOIN sl_tf_post_tags pt ON p.id = pt.post_id
             GROUP BY p.id, p.title
             ORDER BY tag_count DESC, p.title"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('PHP and MySQL Guide', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['tag_count']);
    }

    /**
     * Count posts per tag (tag cloud data).
     */
    public function testPostCountPerTag(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS tag, COUNT(pt.post_id) AS post_count
             FROM sl_tf_tags t
             LEFT JOIN sl_tf_post_tags pt ON t.id = pt.tag_id
             GROUP BY t.id, t.name
             ORDER BY post_count DESC, t.name"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('sql', $rows[0]['tag']);
        $this->assertEquals(3, (int) $rows[0]['post_count']);
    }

    /**
     * Add a tag to a post, then verify the filter reflects the change.
     */
    public function testAddTagAndRequery(): void
    {
        // Post 1 only has 'php'. Add 'sql' tag.
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (1, 2)');

        // Now post 1 should also appear in "has all of php+sql" filter
        $rows = $this->ztdQuery(
            "SELECT p.title
             FROM sl_tf_posts p
             JOIN sl_tf_post_tags pt ON p.id = pt.post_id
             JOIN sl_tf_tags t ON pt.tag_id = t.id
             WHERE t.name IN ('php', 'sql')
             GROUP BY p.id, p.title
             HAVING COUNT(DISTINCT t.id) = 2
             ORDER BY p.title"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertSame('PHP and MySQL Guide', $rows[1]['title']);
    }

    /**
     * Remove a tag from a post, verify the change.
     */
    public function testRemoveTagAndRequery(): void
    {
        // Remove 'sql' tag from post 3
        $this->pdo->exec('DELETE FROM sl_tf_post_tags WHERE post_id = 3 AND tag_id = 2');

        // Post 3 should no longer appear in sql filter
        $rows = $this->ztdQuery(
            "SELECT p.title
             FROM sl_tf_posts p
             JOIN sl_tf_post_tags pt ON p.id = pt.post_id
             JOIN sl_tf_tags t ON pt.tag_id = t.id
             WHERE t.name = 'sql'
             ORDER BY p.title"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Advanced SQL Queries', $rows[0]['title']);
        $this->assertSame('Database Design Tips', $rows[1]['title']);
    }

    /**
     * Exclude posts with a specific tag using NOT EXISTS.
     */
    public function testExcludePostsByTag(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.title
             FROM sl_tf_posts p
             WHERE p.status = 'published'
               AND NOT EXISTS (
                   SELECT 1 FROM sl_tf_post_tags pt
                   JOIN sl_tf_tags t ON pt.tag_id = t.id
                   WHERE pt.post_id = p.id AND t.name = 'php'
               )
             ORDER BY p.title"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Advanced SQL Queries', $rows[0]['title']);
        $this->assertSame('Database Design Tips', $rows[1]['title']);
    }

    /**
     * Prepared statement: filter by tag name with parameter.
     */
    public function testPreparedTagFilter(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT p.title
             FROM sl_tf_posts p
             JOIN sl_tf_post_tags pt ON p.id = pt.post_id
             JOIN sl_tf_tags t ON pt.tag_id = t.id
             WHERE t.name = ?
             ORDER BY p.created_at DESC'
        );

        $stmt->execute(['sql']);
        $sqlPosts = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $sqlPosts);
        $this->assertSame('Database Design Tips', $sqlPosts[0]);
    }

    /**
     * Physical isolation: tags added in ZTD do not reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Add a new tag and assign it
        $this->pdo->exec("INSERT INTO sl_tf_tags VALUES (10, 'new-tag')");
        $this->pdo->exec('INSERT INTO sl_tf_post_tags VALUES (1, 10)');

        // Visible in ZTD
        $rows = $this->ztdQuery(
            "SELECT t.name FROM sl_tf_tags t
             JOIN sl_tf_post_tags pt ON t.id = pt.tag_id
             WHERE pt.post_id = 1 ORDER BY t.name"
        );
        $this->assertCount(2, $rows);

        // Not visible after disable
        $this->pdo->disableZtd();
        $rows = $this->pdo->query(
            "SELECT t.name FROM sl_tf_tags t
             JOIN sl_tf_post_tags pt ON t.id = pt.tag_id
             WHERE pt.post_id = 1 ORDER BY t.name"
        )->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows); // Physical tables are empty
    }
}
