<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that SQL keywords embedded in data values are handled correctly by the
 * ZTD CTE rewriter on MySQLi. Verifies that keywords like SELECT, DROP, INSERT,
 * DELETE, etc. in column values do not confuse the query parser or rewriter.
 * @spec SPEC-10.2.98
 */
class SqlKeywordInDataTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_skd_articles (
            id INT PRIMARY KEY,
            title VARCHAR(500),
            body TEXT,
            tags VARCHAR(500)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_skd_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_skd_articles VALUES (1, 'How to SELECT the Best Database', 'When you SELECT a database, consider performance and SELECT features carefully.', 'SELECT,database,tutorial')");
        $this->mysqli->query("INSERT INTO mi_skd_articles VALUES (2, 'DROP Everything and Learn SQL', 'Don''t DROP your old habits. Instead DROP into a new mindset about data.', 'DROP,learning,SQL')");
        $this->mysqli->query("INSERT INTO mi_skd_articles VALUES (3, 'INSERT Yourself Into the Community', 'To INSERT yourself into tech, you need to DELETE fear and UPDATE your skills.', 'INSERT,community,growth')");
        $this->mysqli->query("INSERT INTO mi_skd_articles VALUES (4, 'UPDATE Your Resume for 2024', 'Make sure to UPDATE contact info and DELETE outdated entries.', 'UPDATE,career,resume')");
        $this->mysqli->query("INSERT INTO mi_skd_articles VALUES (5, 'DELETE Old Habits', 'To DELETE bad patterns, first CREATE new ones. ALTER your approach daily.', 'DELETE,habits,ALTER')");
    }

    /**
     * Seed data with SQL keywords is stored and retrieved correctly.
     * @spec SPEC-10.2.98
     */
    public function testInsertAndSelectWithSqlKeywords(): void
    {
        $rows = $this->ztdQuery("SELECT title, tags FROM mi_skd_articles ORDER BY id");

        $this->assertCount(5, $rows);
        $this->assertSame('How to SELECT the Best Database', $rows[0]['title']);
        $this->assertSame('SELECT,database,tutorial', $rows[0]['tags']);
        $this->assertSame('DROP Everything and Learn SQL', $rows[1]['title']);
        $this->assertSame('DELETE Old Habits', $rows[4]['title']);
    }

    /**
     * LIKE with a SQL keyword pattern matches correctly.
     * @spec SPEC-10.2.98
     */
    public function testLikeWithSqlKeyword(): void
    {
        $rows = $this->ztdQuery("
            SELECT id, title FROM mi_skd_articles
            WHERE title LIKE '%SELECT%'
            ORDER BY id
        ");

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertStringContainsString('SELECT', $rows[0]['title']);
    }

    /**
     * Prepared statement with SQL keyword as a bound value.
     * @spec SPEC-10.2.98
     */
    public function testPreparedWithSqlKeywordValue(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM mi_skd_articles WHERE tags LIKE ?",
            ['%DROP%']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
    }

    /**
     * UPDATE a row with a value containing SQL keywords.
     * @spec SPEC-10.2.98
     */
    public function testUpdateWithSqlKeywordValue(): void
    {
        $this->mysqli->query("UPDATE mi_skd_articles SET title = 'CREATE TABLE or DROP TABLE: A Guide' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT title FROM mi_skd_articles WHERE id = 1");

        $this->assertCount(1, $rows);
        $this->assertSame('CREATE TABLE or DROP TABLE: A Guide', $rows[0]['title']);
    }

    /**
     * Insert a value that looks like a complete SQL statement.
     * @spec SPEC-10.2.98
     */
    public function testInsertSqlStatementAsValue(): void
    {
        $this->mysqli->query("INSERT INTO mi_skd_articles VALUES (6, 'SQL Injection Prevention', 'SELECT * FROM users; DROP TABLE users;-- is a classic attack vector.', 'security,injection')");

        $rows = $this->ztdQuery("SELECT body FROM mi_skd_articles WHERE id = 6");

        $this->assertCount(1, $rows);
        $this->assertSame('SELECT * FROM users; DROP TABLE users;-- is a classic attack vector.', $rows[0]['body']);

        // Verify the original table still exists (wasn't actually dropped)
        $rows2 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_skd_articles");
        $this->assertEquals(6, (int) $rows2[0]['cnt']);
    }

    /**
     * Physical table remains empty — all mutations are in ZTD shadow store.
     * @spec SPEC-10.2.98
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_skd_articles VALUES (6, 'Extra', 'body', 'tags')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_skd_articles");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_skd_articles');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
