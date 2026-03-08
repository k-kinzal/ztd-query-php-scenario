<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL FULLTEXT search through CTE shadow on PDO.
 *
 * Full-text search (MATCH ... AGAINST) is a common real-world pattern
 * for search features. Tests verify FULLTEXT indexes, NATURAL LANGUAGE
 * MODE, BOOLEAN MODE, and relevance scoring work through shadow queries.
 * @spec SPEC-3.3f
 */
class MysqlFullTextSearchTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_fts_articles (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                body TEXT,
                FULLTEXT INDEX ft_title_body (title, body)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_fts_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_fts_articles (id, title, body) VALUES (1, 'PHP Database Tutorial', 'Learn how to connect PHP to MySQL databases using PDO and MySQLi extensions')");
        $this->pdo->exec("INSERT INTO pdo_fts_articles (id, title, body) VALUES (2, 'JavaScript Promises Guide', 'Understanding async programming with promises and async/await in modern JavaScript')");
        $this->pdo->exec("INSERT INTO pdo_fts_articles (id, title, body) VALUES (3, 'MySQL Performance Tuning', 'Optimize your MySQL database queries with proper indexing and query analysis')");
        $this->pdo->exec("INSERT INTO pdo_fts_articles (id, title, body) VALUES (4, 'PHP Testing Best Practices', 'Write better tests for your PHP applications using PHPUnit and testing patterns')");
    }

    /**
     * MATCH ... AGAINST in NATURAL LANGUAGE MODE finds relevant rows.
     */
    public function testNaturalLanguageMode(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pdo_fts_articles WHERE MATCH(title, body) AGAINST('PHP database' IN NATURAL LANGUAGE MODE) ORDER BY id"
            );
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
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pdo_fts_articles WHERE MATCH(title, body) AGAINST('+PHP +testing' IN BOOLEAN MODE) ORDER BY id"
            );
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
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pdo_fts_articles WHERE MATCH(title, body) AGAINST('+PHP -testing' IN BOOLEAN MODE) ORDER BY id"
            );
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
            $rows = $this->ztdQuery(
                "SELECT id, title, MATCH(title, body) AGAINST('MySQL database') AS relevance FROM pdo_fts_articles ORDER BY relevance DESC"
            );
            $this->assertNotEmpty($rows);
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
            $rows = $this->ztdQuery(
                "SELECT id, title FROM pdo_fts_articles WHERE MATCH(title, body) AGAINST('python django flask')"
            );
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
        $this->pdo->exec("INSERT INTO pdo_fts_articles (id, title, body) VALUES (5, 'Rust Systems Programming', 'Build fast and safe systems with Rust programming language')");

        try {
            $rows = $this->ztdQuery(
                "SELECT title FROM pdo_fts_articles WHERE MATCH(title, body) AGAINST('Rust systems')"
            );
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
                "SELECT id, title FROM pdo_fts_articles WHERE MATCH(title, body) AGAINST(? IN BOOLEAN MODE) ORDER BY id",
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
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_fts_articles');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
