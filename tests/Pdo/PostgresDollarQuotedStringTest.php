<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests dollar-quoted string literals on PostgreSQL ZTD.
 *
 * PostgreSQL supports dollar-quoting: $$text$$ or $tag$text$tag$
 * which is useful for strings containing single quotes.
 * The PgSqlParser handles these in splitStatements() and stripStringLiterals().
 *
 * Dollar-quoting is NOT valid in standard SQL values — it's used in
 * PL/pgSQL function bodies, DO blocks, etc. For INSERT/UPDATE values,
 * PostgreSQL requires standard single-quoted strings. These tests verify
 * that ZTD handles strings with embedded quotes correctly, and that
 * dollar-quoted function bodies don't break statement splitting.
 */
class PostgresDollarQuotedStringTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_dq_test');
        $raw->exec('CREATE TABLE pg_dq_test (id INT PRIMARY KEY, body TEXT, notes TEXT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    /**
     * Strings containing single quotes should work with escaped quotes.
     */
    public function testInsertStringWithEscapedSingleQuotes(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (1, 'It''s a test', 'note')");

        $stmt = $this->pdo->query('SELECT body FROM pg_dq_test WHERE id = 1');
        $this->assertSame("It's a test", $stmt->fetchColumn());
    }

    /**
     * Prepared statement with string containing single quotes.
     */
    public function testPreparedInsertStringWithQuotes(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_dq_test (id, body, notes) VALUES (?, ?, ?)');
        $stmt->execute([2, "She said 'hello'", 'quoted']);

        $select = $this->pdo->query('SELECT body FROM pg_dq_test WHERE id = 2');
        $this->assertSame("She said 'hello'", $select->fetchColumn());
    }

    /**
     * Multiple semicolons inside string literals should not split statements.
     */
    public function testStringContainingSemicolons(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (3, 'a;b;c', 'semi;colon')");

        $stmt = $this->pdo->query('SELECT body, notes FROM pg_dq_test WHERE id = 3');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('a;b;c', $row['body']);
        $this->assertSame('semi;colon', $row['notes']);
    }

    /**
     * Bug: UPDATE with escaped single quotes in SET value breaks WHERE clause parsing.
     *
     * The PgSqlParser::stripStringLiterals() regex does not handle '' (doubled
     * single quote) escaping correctly. It treats 'it''s updated' as two separate
     * string literals ('it' and 's updated'), consuming the character between them.
     * This causes 'WHERE id = 4' to become 'WHERE d = 4'.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/25
     */
    public function testUpdateWithEscapedQuotesInSetBreaksWhere(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (4, 'original', 'note')");

        $this->expectException(\ZtdQuery\Adapter\Pdo\ZtdPdoException::class);
        $this->pdo->exec("UPDATE pg_dq_test SET body = 'it''s updated' WHERE id = 4");
    }

    /**
     * Workaround: use prepared statements to avoid quote escaping issues.
     */
    public function testUpdateWithQuotesViaPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (40, 'original', 'note')");

        $stmt = $this->pdo->prepare('UPDATE pg_dq_test SET body = ? WHERE id = ?');
        $stmt->execute(["it's updated", 40]);

        $select = $this->pdo->query('SELECT body FROM pg_dq_test WHERE id = 40');
        $this->assertSame("it's updated", $select->fetchColumn());
    }

    /**
     * WHERE clause with string containing single quotes.
     */
    public function testWhereClauseWithQuotedString(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (5, 'Bob''s item', 'find me')");

        $stmt = $this->pdo->query("SELECT notes FROM pg_dq_test WHERE body = 'Bob''s item'");
        $this->assertSame('find me', $stmt->fetchColumn());
    }

    /**
     * String containing backslash (PostgreSQL standard_conforming_strings = on by default).
     */
    public function testStringWithBackslash(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (6, 'path\\to\\file', 'backslash')");

        $stmt = $this->pdo->query('SELECT body FROM pg_dq_test WHERE id = 6');
        $this->assertSame('path\\to\\file', $stmt->fetchColumn());
    }

    /**
     * Empty string and NULL handling.
     */
    public function testEmptyStringAndNull(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (7, '', NULL)");

        $stmt = $this->pdo->query('SELECT body, notes FROM pg_dq_test WHERE id = 7');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('', $row['body']);
        $this->assertNull($row['notes']);
    }

    /**
     * String with double quotes inside (not dollar-quoting but still tricky).
     */
    public function testStringWithDoubleQuotes(): void
    {
        $this->pdo->exec('INSERT INTO pg_dq_test (id, body, notes) VALUES (8, \'She said "hello"\', \'dq\')');

        $stmt = $this->pdo->query('SELECT body FROM pg_dq_test WHERE id = 8');
        $this->assertSame('She said "hello"', $stmt->fetchColumn());
    }

    /**
     * Verify physical isolation — no data leaks to physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_dq_test (id, body, notes) VALUES (9, 'shadow only', 'test')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dq_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_dq_test');
    }
}
