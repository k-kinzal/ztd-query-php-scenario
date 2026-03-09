<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests string operation edge cases through the CTE shadow store (PostgreSQL).
 *
 * Verifies that string functions (concatenation, SUBSTRING, LENGTH, UPPER/LOWER,
 * TRIM, REPLACE) work correctly when the CTE rewriter is active, including
 * edge cases with single quotes, unicode, empty strings, long text, and newlines.
 *
 * NOTE: TRIM(chars FROM string) syntax is deliberately avoided due to known
 * issue SPEC-11.PG-UPDATE-SET-FROM-KEYWORD where the PostgreSQL parser treats
 * FROM in TRIM(x FROM y) as the FROM clause keyword.
 *
 * @spec SPEC-3.3
 */
class PostgresStringOperationTest extends AbstractPostgresPdoTestCase
{
    private string $longText;

    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_str_messages (
            id INT PRIMARY KEY,
            sender TEXT NOT NULL,
            content TEXT NOT NULL,
            tag TEXT NOT NULL
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_str_messages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->longText = str_repeat('abcdefghij', 55); // 550 chars

        $this->pdo->exec("INSERT INTO pg_str_messages VALUES (1, 'Alice', 'Hello World', 'greeting')");
        $this->pdo->exec("INSERT INTO pg_str_messages VALUES (2, 'O''Brien', 'It''s a test', 'quote')");
        $this->pdo->exec("INSERT INTO pg_str_messages VALUES (3, 'Renée', 'Welcome to café naïve 日本語', 'unicode')");
        $this->pdo->exec("INSERT INTO pg_str_messages VALUES (4, 'Empty', '', 'empty')");
        $this->pdo->exec("INSERT INTO pg_str_messages VALUES (5, 'Verbose', '{$this->longText}', 'long')");
        $this->pdo->exec("INSERT INTO pg_str_messages VALUES (6, 'Liner', E'line1\\nline2\\nline3', 'newline')");
    }

    /**
     * String concatenation with || operator.
     */
    public function testStringConcatenation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sender || ': ' || content AS full_message
             FROM pg_str_messages
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice: Hello World', $rows[0]['full_message']);
    }

    /**
     * Concatenation with single-quote content (O'Brien).
     */
    public function testConcatenationWithQuotes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sender || ' says: ' || content AS full_message
             FROM pg_str_messages
             WHERE id = 2"
        );

        $this->assertCount(1, $rows);
        $this->assertSame("O'Brien says: It's a test", $rows[0]['full_message']);
    }

    /**
     * SUBSTRING extracts substrings correctly.
     */
    public function testSubstring(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTRING(content, 1, 5) AS sub
             FROM pg_str_messages
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Hello', $rows[0]['sub']);
    }

    /**
     * SUBSTRING on unicode text.
     */
    public function testSubstringUnicode(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTRING(content, 12, 4) AS sub
             FROM pg_str_messages
             WHERE id = 3"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('café', $rows[0]['sub']);
    }

    /**
     * LENGTH function returns correct lengths.
     */
    public function testLength(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, LENGTH(content) AS len
             FROM pg_str_messages
             WHERE id IN (1, 4, 5)
             ORDER BY id"
        );

        $this->assertCount(3, $rows);
        // "Hello World" = 11
        $this->assertEquals(11, (int) $rows[0]['len']);
        // Empty string = 0
        $this->assertEquals(0, (int) $rows[1]['len']);
        // Long text = 550
        $this->assertEquals(550, (int) $rows[2]['len']);
    }

    /**
     * UPPER and LOWER functions.
     */
    public function testUpperLower(): void
    {
        $rows = $this->ztdQuery(
            "SELECT UPPER(content) AS up, LOWER(content) AS lo
             FROM pg_str_messages
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('HELLO WORLD', $rows[0]['up']);
        $this->assertSame('hello world', $rows[0]['lo']);
    }

    /**
     * TRIM, LTRIM, RTRIM functions.
     *
     * Uses TRIM(string), LTRIM(string), RTRIM(string) — NOT TRIM(chars FROM string)
     * to avoid SPEC-11.PG-UPDATE-SET-FROM-KEYWORD.
     */
    public function testTrimFunctions(): void
    {
        $this->pdo->exec("INSERT INTO pg_str_messages VALUES (10, 'Padded', '  spaced  ', 'trim')");

        $rows = $this->ztdQuery(
            "SELECT TRIM(content) AS trimmed,
                    LTRIM(content) AS ltrimmed,
                    RTRIM(content) AS rtrimmed
             FROM pg_str_messages
             WHERE id = 10"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('spaced', $rows[0]['trimmed']);
        $this->assertSame('spaced  ', $rows[0]['ltrimmed']);
        $this->assertSame('  spaced', $rows[0]['rtrimmed']);
    }

    /**
     * REPLACE function.
     */
    public function testReplace(): void
    {
        $rows = $this->ztdQuery(
            "SELECT REPLACE(content, 'World', 'PostgreSQL') AS replaced
             FROM pg_str_messages
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Hello PostgreSQL', $rows[0]['replaced']);
    }

    /**
     * REPLACE on content with single quotes.
     */
    public function testReplaceWithQuotes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT REPLACE(content, 'test', 'success') AS replaced
             FROM pg_str_messages
             WHERE id = 2"
        );

        $this->assertCount(1, $rows);
        $this->assertSame("It's a success", $rows[0]['replaced']);
    }

    /**
     * Single-quote preservation: O'Brien inserted and queried back correctly.
     */
    public function testSingleQuotePreservation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sender, content
             FROM pg_str_messages
             WHERE id = 2"
        );

        $this->assertCount(1, $rows);
        $this->assertSame("O'Brien", $rows[0]['sender']);
        $this->assertSame("It's a test", $rows[0]['content']);
    }

    /**
     * Single-quote in WHERE clause filter.
     */
    public function testSingleQuoteInWhereClause(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM pg_str_messages WHERE sender = 'O''Brien'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
    }

    /**
     * Unicode text preservation through shadow store.
     */
    public function testUnicodePreservation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sender, content
             FROM pg_str_messages
             WHERE id = 3"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Renée', $rows[0]['sender']);
        $this->assertSame('Welcome to café naïve 日本語', $rows[0]['content']);
    }

    /**
     * Physical isolation: underlying table has no rows.
     */
    public function testPhysicalIsolation(): void
    {
        // ZTD sees all seeded rows
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_str_messages");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_str_messages")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
