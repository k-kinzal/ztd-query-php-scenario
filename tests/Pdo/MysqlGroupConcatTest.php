<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL GROUP_CONCAT() function on shadow data.
 *
 * GROUP_CONCAT is one of MySQL's most distinctive and widely used
 * aggregate functions. Applications use it to aggregate related values
 * into comma-separated lists (tags, permissions, category paths).
 * The CTE rewriter must handle the function's special syntax including
 * SEPARATOR, ORDER BY, and DISTINCT within the function call.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class MysqlGroupConcatTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_gc_articles (
                id INT PRIMARY KEY,
                title VARCHAR(200) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_gc_tags (
                article_id INT NOT NULL,
                tag VARCHAR(50) NOT NULL,
                PRIMARY KEY (article_id, tag)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_gc_tags', 'my_gc_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_gc_articles VALUES (1, 'PHP Best Practices')");
        $this->ztdExec("INSERT INTO my_gc_articles VALUES (2, 'MySQL Performance')");
        $this->ztdExec("INSERT INTO my_gc_articles VALUES (3, 'REST API Design')");

        $this->ztdExec("INSERT INTO my_gc_tags VALUES (1, 'php')");
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (1, 'backend')");
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (1, 'best-practices')");
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (2, 'mysql')");
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (2, 'performance')");
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (2, 'backend')");
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (3, 'api')");
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (3, 'rest')");
    }

    /**
     * Basic GROUP_CONCAT with JOIN on shadow data.
     */
    public function testGroupConcatBasic(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, GROUP_CONCAT(t.tag ORDER BY t.tag) AS tags
             FROM my_gc_articles a
             JOIN my_gc_tags t ON t.article_id = a.id
             GROUP BY a.id, a.title
             ORDER BY a.title"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('MySQL Performance', $rows[0]['title']);
        $this->assertSame('backend,mysql,performance', $rows[0]['tags']);
        $this->assertSame('PHP Best Practices', $rows[1]['title']);
        $this->assertSame('backend,best-practices,php', $rows[1]['tags']);
    }

    /**
     * GROUP_CONCAT with SEPARATOR.
     */
    public function testGroupConcatWithSeparator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, GROUP_CONCAT(t.tag ORDER BY t.tag SEPARATOR ' | ') AS tags
             FROM my_gc_articles a
             JOIN my_gc_tags t ON t.article_id = a.id
             GROUP BY a.id, a.title
             ORDER BY a.title"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('backend | mysql | performance', $rows[0]['tags']);
    }

    /**
     * GROUP_CONCAT with DISTINCT.
     */
    public function testGroupConcatDistinct(): void
    {
        // Add duplicate tags (same tag on different articles)
        // Query all unique tags across all articles
        $rows = $this->ztdQuery(
            "SELECT GROUP_CONCAT(DISTINCT t.tag ORDER BY t.tag) AS all_tags
             FROM my_gc_tags t"
        );

        $this->assertCount(1, $rows);
        $allTags = explode(',', $rows[0]['all_tags']);
        $this->assertContains('backend', $allTags);
        $this->assertContains('php', $allTags);
        $this->assertContains('api', $allTags);
        // 'backend' should appear only once due to DISTINCT
        $this->assertEquals(1, substr_count($rows[0]['all_tags'], 'backend'));
    }

    /**
     * GROUP_CONCAT in HAVING clause.
     */
    public function testGroupConcatInHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.title, GROUP_CONCAT(t.tag ORDER BY t.tag) AS tags
             FROM my_gc_articles a
             JOIN my_gc_tags t ON t.article_id = a.id
             GROUP BY a.id, a.title
             HAVING GROUP_CONCAT(t.tag ORDER BY t.tag) LIKE '%backend%'
             ORDER BY a.title"
        );

        // Articles with 'backend' tag: PHP Best Practices, MySQL Performance
        $this->assertCount(2, $rows);
        $this->assertSame('MySQL Performance', $rows[0]['title']);
        $this->assertSame('PHP Best Practices', $rows[1]['title']);
    }

    /**
     * GROUP_CONCAT after shadow mutation.
     */
    public function testGroupConcatAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO my_gc_tags VALUES (3, 'backend')");

        $rows = $this->ztdQuery(
            "SELECT a.title, GROUP_CONCAT(t.tag ORDER BY t.tag) AS tags
             FROM my_gc_articles a
             JOIN my_gc_tags t ON t.article_id = a.id
             WHERE a.id = 3
             GROUP BY a.id, a.title"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('api,backend,rest', $rows[0]['tags']);
    }

    /**
     * GROUP_CONCAT with prepared params.
     */
    public function testGroupConcatWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT a.title, GROUP_CONCAT(t.tag ORDER BY t.tag) AS tags
             FROM my_gc_articles a
             JOIN my_gc_tags t ON t.article_id = a.id
             WHERE a.id = ?
             GROUP BY a.id, a.title",
            [1]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('backend,best-practices,php', $rows[0]['tags']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM my_gc_articles')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
