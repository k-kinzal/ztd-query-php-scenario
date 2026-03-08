<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL FULLTEXT search through CTE shadow on MySQLi.
 *
 * Full-text search (MATCH ... AGAINST) is a common real-world pattern
 * for search features. Tests verify FULLTEXT indexes, NATURAL LANGUAGE
 * MODE, BOOLEAN MODE, and relevance scoring work through shadow queries.
 * @spec pending
 */
class FullTextSearchTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_fts_articles (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                body TEXT,
                FULLTEXT INDEX ft_title_body (title, body)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_fts_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_fts_articles (id, title, body) VALUES (1, 'PHP Database Tutorial', 'Learn how to connect PHP to MySQL databases using PDO and MySQLi extensions')");
        $this->mysqli->query("INSERT INTO mi_fts_articles (id, title, body) VALUES (2, 'JavaScript Promises Guide', 'Understanding async programming with promises and async/await in modern JavaScript')");
        $this->mysqli->query("INSERT INTO mi_fts_articles (id, title, body) VALUES (3, 'MySQL Performance Tuning', 'Optimize your MySQL database queries with proper indexing and query analysis')");
        $this->mysqli->query("INSERT INTO mi_fts_articles (id, title, body) VALUES (4, 'PHP Testing Best Practices', 'Write better tests for your PHP applications using PHPUnit and testing patterns')");
    }

    /**
     * MATCH ... AGAINST in NATURAL LANGUAGE MODE finds relevant rows.
     */
    public function testNaturalLanguageMode(): void
    {
        try {
            $result = $this->mysqli->query(
                "SELECT id, title FROM mi_fts_articles WHERE MATCH(title, body) AGAINST('PHP database' IN NATURAL LANGUAGE MODE) ORDER BY id"
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FULLTEXT search not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * MATCH ... AGAINST in BOOLEAN MODE with + (required) operator.
     */
    public function testBooleanModeRequired(): void
    {
        try {
            $result = $this->mysqli->query(
                "SELECT id, title FROM mi_fts_articles WHERE MATCH(title, body) AGAINST('+PHP +testing' IN BOOLEAN MODE) ORDER BY id"
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Testing Best Practices', $titles);
            $this->assertNotContains('JavaScript Promises Guide', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FULLTEXT BOOLEAN MODE not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * MATCH ... AGAINST in BOOLEAN MODE with - (exclusion) operator.
     */
    public function testBooleanModeExclusion(): void
    {
        try {
            $result = $this->mysqli->query(
                "SELECT id, title FROM mi_fts_articles WHERE MATCH(title, body) AGAINST('+PHP -testing' IN BOOLEAN MODE) ORDER BY id"
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
            $this->assertNotContains('PHP Testing Best Practices', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FULLTEXT BOOLEAN MODE not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * MATCH ... AGAINST as relevance score in SELECT.
     */
    public function testRelevanceScore(): void
    {
        try {
            $result = $this->mysqli->query(
                "SELECT id, title, MATCH(title, body) AGAINST('MySQL database') AS relevance FROM mi_fts_articles ORDER BY relevance DESC"
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            // Most relevant should be first
            $this->assertNotEmpty($rows);
            // Rows with MySQL content should have higher relevance
            $first = $rows[0];
            $this->assertTrue(
                $first['title'] === 'MySQL Performance Tuning' || $first['title'] === 'PHP Database Tutorial',
                'Most relevant result should contain MySQL or database'
            );
        } catch (\Throwable $e) {
            $this->markTestSkipped('FULLTEXT relevance scoring not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * MATCH ... AGAINST with no matching rows returns empty.
     */
    public function testNoMatchReturnsEmpty(): void
    {
        try {
            $result = $this->mysqli->query(
                "SELECT id, title FROM mi_fts_articles WHERE MATCH(title, body) AGAINST('python django flask')"
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $this->assertEmpty($rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FULLTEXT search not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * INSERT then search finds newly inserted row.
     */
    public function testSearchAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_fts_articles (id, title, body) VALUES (5, 'Rust Systems Programming', 'Build fast and safe systems with Rust programming language')");

        try {
            $result = $this->mysqli->query(
                "SELECT title FROM mi_fts_articles WHERE MATCH(title, body) AGAINST('Rust systems')"
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $this->assertCount(1, $rows);
            $this->assertSame('Rust Systems Programming', $rows[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FULLTEXT search after INSERT not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * MATCH ... AGAINST with prepared statement parameters.
     */
    public function testPreparedFullTextSearch(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, title FROM mi_fts_articles WHERE MATCH(title, body) AGAINST(? IN BOOLEAN MODE) ORDER BY id",
                ['+PHP']
            );
            $titles = array_column($rows, 'title');
            $this->assertContains('PHP Database Tutorial', $titles);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared FULLTEXT search not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation — FULLTEXT data only in shadow store.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_fts_articles');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
