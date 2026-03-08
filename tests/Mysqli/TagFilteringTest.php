<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests many-to-many relationship query patterns through ZTD shadow store.
 * Simulates blog/CMS tagging: posts have multiple tags, users filter by tags.
 * @spec SPEC-3.3
 */
class TagFilteringTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_tf_posts (id INT PRIMARY KEY, title VARCHAR(255), status VARCHAR(20), created_at DATE)',
            'CREATE TABLE mi_tf_tags (id INT PRIMARY KEY, name VARCHAR(50) UNIQUE)',
            'CREATE TABLE mi_tf_post_tags (post_id INT, tag_id INT, PRIMARY KEY (post_id, tag_id))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_tf_post_tags', 'mi_tf_tags', 'mi_tf_posts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_tf_posts VALUES (1, 'Getting Started with PHP', 'published', '2024-01-10')");
        $this->mysqli->query("INSERT INTO mi_tf_posts VALUES (2, 'Advanced SQL Queries', 'published', '2024-02-15')");
        $this->mysqli->query("INSERT INTO mi_tf_posts VALUES (3, 'PHP and MySQL Guide', 'published', '2024-03-20')");
        $this->mysqli->query("INSERT INTO mi_tf_posts VALUES (4, 'JavaScript Basics', 'draft', '2024-04-05')");
        $this->mysqli->query("INSERT INTO mi_tf_posts VALUES (5, 'Database Design Tips', 'published', '2024-05-12')");

        $this->mysqli->query("INSERT INTO mi_tf_tags VALUES (1, 'php')");
        $this->mysqli->query("INSERT INTO mi_tf_tags VALUES (2, 'sql')");
        $this->mysqli->query("INSERT INTO mi_tf_tags VALUES (3, 'mysql')");
        $this->mysqli->query("INSERT INTO mi_tf_tags VALUES (4, 'javascript')");
        $this->mysqli->query("INSERT INTO mi_tf_tags VALUES (5, 'design')");

        // Post 1: php
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (1, 1)');
        // Post 2: sql
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (2, 2)');
        // Post 3: php, sql, mysql
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (3, 1)');
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (3, 2)');
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (3, 3)');
        // Post 4: javascript
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (4, 4)');
        // Post 5: sql, design
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (5, 2)');
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (5, 5)');
    }

    /**
     * Filter posts by a single tag using JOIN.
     */
    public function testFilterBySingleTag(): void
    {
        $stmt = $this->mysqli->prepare(
            'SELECT p.title
             FROM mi_tf_posts p
             JOIN mi_tf_post_tags pt ON p.id = pt.post_id
             JOIN mi_tf_tags t ON pt.tag_id = t.id
             WHERE t.name = ? AND p.status = ?
             ORDER BY p.title'
        );
        $stmt->bind_param('ss', $tag, $status);
        $tag = 'php';
        $status = 'published';
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Getting Started with PHP', $rows[0]['title']);
        $this->assertSame('PHP and MySQL Guide', $rows[1]['title']);
    }

    /**
     * Filter posts that have ALL specified tags (intersection / "AND" logic).
     * Uses GROUP BY + HAVING COUNT = N pattern.
     */
    public function testFilterByAllTags(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.title
             FROM mi_tf_posts p
             JOIN mi_tf_post_tags pt ON p.id = pt.post_id
             JOIN mi_tf_tags t ON pt.tag_id = t.id
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
             FROM mi_tf_posts p
             JOIN mi_tf_post_tags pt ON p.id = pt.post_id
             JOIN mi_tf_tags t ON pt.tag_id = t.id
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
             FROM mi_tf_posts p
             LEFT JOIN mi_tf_post_tags pt ON p.id = pt.post_id
             GROUP BY p.id, p.title
             ORDER BY tag_count DESC, p.title"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('PHP and MySQL Guide', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['tag_count']);
    }

    /**
     * Add a tag to a post, then verify the filter reflects the change.
     */
    public function testAddTagAndRequery(): void
    {
        // Post 1 only has 'php'. Add 'sql' tag.
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (1, 2)');

        // Now post 1 should also appear in "has all of php+sql" filter
        $rows = $this->ztdQuery(
            "SELECT p.title
             FROM mi_tf_posts p
             JOIN mi_tf_post_tags pt ON p.id = pt.post_id
             JOIN mi_tf_tags t ON pt.tag_id = t.id
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
     * Exclude posts with a specific tag using NOT EXISTS.
     */
    public function testExcludePostsByTag(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.title
             FROM mi_tf_posts p
             WHERE p.status = 'published'
               AND NOT EXISTS (
                   SELECT 1 FROM mi_tf_post_tags pt
                   JOIN mi_tf_tags t ON pt.tag_id = t.id
                   WHERE pt.post_id = p.id AND t.name = 'php'
               )
             ORDER BY p.title"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Advanced SQL Queries', $rows[0]['title']);
        $this->assertSame('Database Design Tips', $rows[1]['title']);
    }

    /**
     * Physical isolation: tags added in ZTD do not reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Add a new tag and assign it
        $this->mysqli->query("INSERT INTO mi_tf_tags VALUES (10, 'new-tag')");
        $this->mysqli->query('INSERT INTO mi_tf_post_tags VALUES (1, 10)');

        // Visible in ZTD
        $rows = $this->ztdQuery(
            "SELECT t.name FROM mi_tf_tags t
             JOIN mi_tf_post_tags pt ON t.id = pt.tag_id
             WHERE pt.post_id = 1 ORDER BY t.name"
        );
        $this->assertCount(2, $rows);

        // Not visible after disable
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_tf_tags');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
