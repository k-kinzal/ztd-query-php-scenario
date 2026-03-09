<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests queries with string literals containing SQL keywords, table names,
 * and parameter placeholders (? and :named).
 *
 * The CTE rewriter must not treat SQL inside string literals as actual SQL.
 * Extends known issue #67 with more complex patterns involving prepared params.
 *
 * @spec SPEC-3.2
 */
class SqliteStringLiteralParamPlaceholderTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE query_log (id INTEGER PRIMARY KEY, query_text TEXT, status TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['query_log'];
    }

    /**
     * String literal containing ? placeholder — actual query also uses prepared params.
     */
    public function testStringLiteralWithQuestionMarkAndRealParam(): void
    {
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (1, 'SELECT * FROM users WHERE id = ?', 'logged')");
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (2, 'DELETE FROM orders WHERE status = ?', 'logged')");

        $stmt = $this->pdo->prepare('SELECT query_text FROM query_log WHERE status = ?');
        $stmt->execute(['logged']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('SELECT * FROM users WHERE id = ?', $rows[0]['query_text']);
        $this->assertSame('DELETE FROM orders WHERE status = ?', $rows[1]['query_text']);
    }

    /**
     * String literal containing :named param — actual query uses named params.
     */
    public function testStringLiteralWithNamedParamAndRealNamedParam(): void
    {
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (1, 'UPDATE users SET name = :name WHERE id = :id', 'pending')");

        $stmt = $this->pdo->prepare('SELECT query_text FROM query_log WHERE status = :status');
        $stmt->execute([':status' => 'pending']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('UPDATE users SET name = :name WHERE id = :id', $rows[0]['query_text']);
    }

    /**
     * INSERT string value containing FROM keyword + table name.
     */
    public function testInsertStringContainingFromAndTableName(): void
    {
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (1, 'SELECT * FROM query_log WHERE id > 0', 'meta')");

        $rows = $this->ztdQuery("SELECT query_text FROM query_log WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('SELECT * FROM query_log WHERE id > 0', $rows[0]['query_text']);
    }

    /**
     * INSERT string containing UPDATE keyword referencing same table.
     */
    public function testInsertStringContainingUpdateSameTable(): void
    {
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (1, 'UPDATE query_log SET status = ''done''', 'pending')");

        $rows = $this->ztdQuery("SELECT query_text FROM query_log WHERE id = 1");
        $this->assertCount(1, $rows);
        // The string should be preserved as-is
        $this->assertStringContainsString('UPDATE query_log', $rows[0]['query_text']);
    }

    /**
     * Prepared INSERT with string value containing ? placeholders.
     */
    public function testPreparedInsertWithStringContainingQuestionMark(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO query_log (id, query_text, status) VALUES (?, ?, ?)');
        $stmt->execute([1, 'SELECT * FROM users WHERE name = ? AND age > ?', 'stored']);

        $rows = $this->ztdQuery("SELECT query_text FROM query_log WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('SELECT * FROM users WHERE name = ? AND age > ?', $rows[0]['query_text']);
    }

    /**
     * LIKE pattern containing ? with actual prepared param.
     */
    public function testLikePatternWithQuestionMarkAndRealParam(): void
    {
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (1, 'WHERE id = ?', 'active')");
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (2, 'plain query', 'active')");

        $stmt = $this->pdo->prepare("SELECT id FROM query_log WHERE query_text LIKE ? AND status = ?");
        $stmt->execute(['%?%', 'active']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
    }

    /**
     * String containing multiple SQL keywords (INSERT, UPDATE, DELETE, SELECT).
     */
    public function testStringWithMultipleSqlKeywords(): void
    {
        $complexSql = 'BEGIN; INSERT INTO t VALUES (1); UPDATE t SET x = 2; DELETE FROM t; SELECT * FROM t; COMMIT;';
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (1, '$complexSql', 'batch')");

        $rows = $this->ztdQuery("SELECT query_text FROM query_log WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame($complexSql, $rows[0]['query_text']);
    }

    /**
     * String literal containing CTE-like syntax (WITH ... AS).
     */
    public function testStringLiteralContainingCTESyntax(): void
    {
        $cteQuery = 'WITH cte AS (SELECT 1) SELECT * FROM cte';
        $this->pdo->exec("INSERT INTO query_log (id, query_text, status) VALUES (1, '$cteQuery', 'stored')");

        $rows = $this->ztdQuery("SELECT query_text FROM query_log WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame($cteQuery, $rows[0]['query_text']);
    }
}
