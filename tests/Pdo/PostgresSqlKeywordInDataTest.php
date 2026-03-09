<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests that SQL reserved keywords appearing as data values (not syntax)
 * are handled correctly through ZTD on PostgreSQL via PDO.
 * @spec SPEC-10.2.98
 */
class PostgresSqlKeywordInDataTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_skd_articles (
            id INTEGER PRIMARY KEY,
            title VARCHAR(500),
            body TEXT,
            tags VARCHAR(500)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_skd_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_skd_articles VALUES (1, 'SELECT your next adventure', 'Join us for a WHERE-inspired journey', 'travel,SELECT,adventure')");
        $this->pdo->exec("INSERT INTO pg_skd_articles VALUES (2, 'How to DROP bad habits', 'DELETE negative thoughts FROM your mind', 'self-help,DROP,DELETE')");
        $this->pdo->exec("INSERT INTO pg_skd_articles VALUES (3, 'INSERT creativity INTO your life', 'UPDATE your daily routine', 'lifestyle,INSERT,UPDATE')");
    }

    /**
     * Insert and select rows where data contains SQL keywords.
     */
    public function testInsertAndSelectWithSqlKeywords(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, title, body, tags FROM pg_skd_articles ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('SELECT your next adventure', $rows[0]['title']);
        $this->assertSame('Join us for a WHERE-inspired journey', $rows[0]['body']);
        $this->assertSame('How to DROP bad habits', $rows[1]['title']);
        $this->assertSame('DELETE negative thoughts FROM your mind', $rows[1]['body']);
        $this->assertSame('INSERT creativity INTO your life', $rows[2]['title']);
        $this->assertSame('UPDATE your daily routine', $rows[2]['body']);
    }

    /**
     * LIKE filter where the search term is a SQL keyword.
     */
    public function testLikeWithSqlKeyword(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, title FROM pg_skd_articles WHERE title LIKE '%DROP%'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('How to DROP bad habits', $rows[0]['title']);
    }

    /**
     * Prepared statement with SQL keywords as bound parameter values.
     */
    public function testPreparedWithSqlKeywordValue(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM pg_skd_articles WHERE tags LIKE ?",
            ['%SELECT%']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertSame('SELECT your next adventure', $rows[0]['title']);
    }

    /**
     * Update a column to a value containing SQL keywords.
     */
    public function testUpdateWithSqlKeywordValue(): void
    {
        $this->pdo->exec("UPDATE pg_skd_articles SET title = 'ALTER TABLE of contents' WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT title FROM pg_skd_articles WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('ALTER TABLE of contents', $rows[0]['title']);
    }

    /**
     * Insert a value that looks like a complete SQL statement.
     */
    public function testInsertSqlStatementAsValue(): void
    {
        $this->pdo->exec("INSERT INTO pg_skd_articles VALUES (4, 'SQL Injection Test', 'SELECT * FROM users; DROP TABLE users;--', 'security')");

        $rows = $this->ztdQuery(
            "SELECT body FROM pg_skd_articles WHERE id = 4"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('SELECT * FROM users; DROP TABLE users;--', $rows[0]['body']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_skd_articles VALUES (5, 'GRANT ALL PRIVILEGES', 'REVOKE access', 'admin')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_skd_articles");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_skd_articles')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
