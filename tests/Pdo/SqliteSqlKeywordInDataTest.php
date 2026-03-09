<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests string values that contain SQL keywords (SELECT, DROP, FROM, WHERE, etc.).
 * Parser must not be confused by keyword-like content in string literals.
 * @spec SPEC-10.2.98
 */
class SqliteSqlKeywordInDataTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_skd_articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            body TEXT,
            tags TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_skd_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_skd_articles VALUES (1, 'SELECT the best products FROM our store', 'Learn how to SELECT items FROM the catalog WHERE quality matters.', 'shopping,SELECT,FROM')");
        $this->pdo->exec("INSERT INTO sl_skd_articles VALUES (2, 'How to DROP bad habits AND pick up new ones', 'You should DROP what does not serve you AND start fresh.', 'self-help,DROP,AND')");
        $this->pdo->exec("INSERT INTO sl_skd_articles VALUES (3, 'WHERE to go on vacation: INSERT adventure INTO your life', 'INSERT yourself INTO new experiences WHERE fun awaits.', 'travel,WHERE,INSERT,INTO')");
        $this->pdo->exec("INSERT INTO sl_skd_articles VALUES (4, 'DELETE negative thoughts; UPDATE your mindset', 'DELETE the old patterns; UPDATE your approach to life.', 'wellness,DELETE,UPDATE')");
        $this->pdo->exec("INSERT INTO sl_skd_articles VALUES (5, 'GROUP BY interest; ORDER BY priority', 'GROUP your tasks BY urgency and ORDER them BY importance.', 'productivity,GROUP,ORDER')");
    }

    /**
     * Verify all data survives the round-trip through ZTD shadow store.
     */
    public function testInsertAndSelectWithSqlKeywords(): void
    {
        $rows = $this->ztdQuery("SELECT id, title, body, tags FROM sl_skd_articles ORDER BY id");

        $this->assertCount(5, $rows);
        $this->assertSame('SELECT the best products FROM our store', $rows[0]['title']);
        $this->assertSame('How to DROP bad habits AND pick up new ones', $rows[1]['title']);
        $this->assertSame('WHERE to go on vacation: INSERT adventure INTO your life', $rows[2]['title']);
        $this->assertSame('DELETE negative thoughts; UPDATE your mindset', $rows[3]['title']);
        $this->assertSame('GROUP BY interest; ORDER BY priority', $rows[4]['title']);
    }

    /**
     * LIKE search with SQL keyword in the pattern.
     */
    public function testLikeWithSqlKeyword(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, title FROM sl_skd_articles WHERE title LIKE '%DROP%'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
        $this->assertSame('How to DROP bad habits AND pick up new ones', $rows[0]['title']);
    }

    /**
     * Prepared statement with a keyword-containing value as parameter.
     */
    public function testPreparedWithSqlKeywordValue(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM sl_skd_articles WHERE title = ?",
            ['DELETE negative thoughts; UPDATE your mindset']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['id']);
    }

    /**
     * Update body to contain more SQL keywords.
     */
    public function testUpdateWithSqlKeywordValue(): void
    {
        $this->pdo->exec(
            "UPDATE sl_skd_articles SET body = 'ALTER TABLE your life; CREATE INDEX on happiness; TRUNCATE TABLE of worries' WHERE id = 1"
        );

        $rows = $this->ztdQuery("SELECT body FROM sl_skd_articles WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame(
            'ALTER TABLE your life; CREATE INDEX on happiness; TRUNCATE TABLE of worries',
            $rows[0]['body']
        );
    }

    /**
     * Insert a value that is a complete SQL statement and verify it's stored literally.
     */
    public function testInsertSqlStatementAsValue(): void
    {
        $sqlStatement = "SELECT * FROM users WHERE id = 1; DROP TABLE users;";
        $stmt = $this->pdo->prepare(
            "INSERT INTO sl_skd_articles VALUES (6, ?, ?, ?)"
        );
        $stmt->execute([$sqlStatement, $sqlStatement, 'sql-injection-test']);

        $rows = $this->ztdQuery("SELECT title, body FROM sl_skd_articles WHERE id = 6");
        $this->assertCount(1, $rows);
        $this->assertSame($sqlStatement, $rows[0]['title']);
        $this->assertSame($sqlStatement, $rows[0]['body']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "UPDATE sl_skd_articles SET title = 'TRUNCATE TABLE everything' WHERE id = 1"
        );
        $this->pdo->exec("INSERT INTO sl_skd_articles VALUES (6, 'New', 'Body', 'tag')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_skd_articles");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_skd_articles')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
