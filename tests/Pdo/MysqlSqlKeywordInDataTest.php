<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that SQL reserved keywords embedded in data values are handled
 * correctly through the ZTD CTE rewriter on MySQL via PDO.
 * Covers INSERT/SELECT with keyword values, LIKE, prepared statements,
 * UPDATE, and full SQL statements stored as data.
 * @spec SPEC-10.2.98
 */
class MysqlSqlKeywordInDataTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_skd_articles (
            id INT PRIMARY KEY,
            title VARCHAR(500),
            body TEXT,
            tags VARCHAR(500)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_skd_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_skd_articles VALUES (1, 'SELECT the Best Framework', 'When you SELECT a framework, consider DROP rates and DELETE policies.', 'SELECT,DROP,DELETE')");
        $this->pdo->exec("INSERT INTO mp_skd_articles VALUES (2, 'How to UPDATE Your Resume', 'INSERT your latest skills and ALTER the format to stand out.', 'UPDATE,INSERT,ALTER')");
        $this->pdo->exec("INSERT INTO mp_skd_articles VALUES (3, 'JOIN Our Community', 'WHERE developers GROUP BY interest and ORDER BY experience.', 'JOIN,WHERE,GROUP BY')");
    }

    /**
     * @spec SPEC-10.2.98
     */
    public function testInsertAndSelectWithSqlKeywords(): void
    {
        $rows = $this->ztdQuery("SELECT title, tags FROM mp_skd_articles ORDER BY id");

        $this->assertCount(3, $rows);
        $this->assertSame('SELECT the Best Framework', $rows[0]['title']);
        $this->assertSame('SELECT,DROP,DELETE', $rows[0]['tags']);
        $this->assertSame('How to UPDATE Your Resume', $rows[1]['title']);
        $this->assertSame('JOIN Our Community', $rows[2]['title']);
    }

    /**
     * @spec SPEC-10.2.98
     */
    public function testLikeWithSqlKeyword(): void
    {
        $rows = $this->ztdQuery("
            SELECT id, title FROM mp_skd_articles
            WHERE title LIKE '%SELECT%'
            ORDER BY id
        ");

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame('SELECT the Best Framework', $rows[0]['title']);
    }

    /**
     * @spec SPEC-10.2.98
     */
    public function testPreparedWithSqlKeywordValue(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title FROM mp_skd_articles WHERE tags LIKE ?",
            ['%UPDATE%']
        );

        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
        $this->assertSame('How to UPDATE Your Resume', $rows[0]['title']);
    }

    /**
     * @spec SPEC-10.2.98
     */
    public function testUpdateWithSqlKeywordValue(): void
    {
        $this->ztdExec("UPDATE mp_skd_articles SET title = 'DROP Everything and READ' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT title FROM mp_skd_articles WHERE id = 1");

        $this->assertCount(1, $rows);
        $this->assertSame('DROP Everything and READ', $rows[0]['title']);
    }

    /**
     * @spec SPEC-10.2.98
     */
    public function testInsertSqlStatementAsValue(): void
    {
        $sqlStatement = "SELECT * FROM users WHERE id = 1; DROP TABLE users; --";
        $stmt = $this->pdo->prepare("INSERT INTO mp_skd_articles VALUES (?, ?, ?, ?)");
        $stmt->execute([4, 'SQL Injection Examples', $sqlStatement, 'security,SQL']);

        $rows = $this->ztdQuery("SELECT body FROM mp_skd_articles WHERE id = 4");

        $this->assertCount(1, $rows);
        $this->assertSame($sqlStatement, $rows[0]['body']);
    }

    /**
     * @spec SPEC-10.2.98
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT INTO mp_skd_articles VALUES (5, 'CREATE TABLE Guide', 'Learn to CREATE TABLE and ALTER TABLE.', 'CREATE,ALTER')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_skd_articles");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_skd_articles')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
