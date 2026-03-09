<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests string operation edge cases through the CTE shadow store.
 *
 * Verifies that string functions (concatenation, SUBSTR, LENGTH, UPPER/LOWER,
 * TRIM, REPLACE) work correctly when the CTE rewriter is active, including
 * edge cases with single quotes, unicode, empty strings, long text, and newlines.
 *
 * @spec SPEC-3.3
 */
class SqliteStringOperationTest extends AbstractSqlitePdoTestCase
{
    private string $longText;

    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_str_messages (
            id INTEGER PRIMARY KEY,
            sender TEXT NOT NULL,
            content TEXT NOT NULL,
            tag TEXT NOT NULL
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_str_messages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->longText = str_repeat('abcdefghij', 55); // 550 chars

        $this->pdo->exec("INSERT INTO sl_str_messages VALUES (1, 'Alice', 'Hello World', 'greeting')");
        $this->pdo->exec("INSERT INTO sl_str_messages VALUES (2, 'O''Brien', 'It''s a test', 'quote')");
        $this->pdo->exec("INSERT INTO sl_str_messages VALUES (3, 'Renée', 'Welcome to café naïve 日本語', 'unicode')");
        $this->pdo->exec("INSERT INTO sl_str_messages VALUES (4, 'Empty', '', 'empty')");
        $this->pdo->exec("INSERT INTO sl_str_messages VALUES (5, 'Verbose', '{$this->longText}', 'long')");
        $this->pdo->exec("INSERT INTO sl_str_messages VALUES (6, 'Liner', 'line1\nline2\nline3', 'newline')");
    }

    /**
     * String concatenation with || operator.
     */
    public function testStringConcatenation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sender || ': ' || content AS full_message
             FROM sl_str_messages
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
             FROM sl_str_messages
             WHERE id = 2"
        );

        $this->assertCount(1, $rows);
        $this->assertSame("O'Brien says: It's a test", $rows[0]['full_message']);
    }

    /**
     * SUBSTR extracts substrings correctly.
     */
    public function testSubstr(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTR(content, 1, 5) AS sub
             FROM sl_str_messages
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Hello', $rows[0]['sub']);
    }

    /**
     * SUBSTR on unicode text.
     */
    public function testSubstrUnicode(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTR(content, 12, 4) AS sub
             FROM sl_str_messages
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
             FROM sl_str_messages
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
             FROM sl_str_messages
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('HELLO WORLD', $rows[0]['up']);
        $this->assertSame('hello world', $rows[0]['lo']);
    }

    /**
     * TRIM, LTRIM, RTRIM functions.
     */
    public function testTrimFunctions(): void
    {
        // Insert a row with padded content via the shadow store
        $this->pdo->exec("INSERT INTO sl_str_messages VALUES (10, 'Padded', '  spaced  ', 'trim')");

        $rows = $this->ztdQuery(
            "SELECT TRIM(content) AS trimmed,
                    LTRIM(content) AS ltrimmed,
                    RTRIM(content) AS rtrimmed
             FROM sl_str_messages
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
            "SELECT REPLACE(content, 'World', 'SQLite') AS replaced
             FROM sl_str_messages
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Hello SQLite', $rows[0]['replaced']);
    }

    /**
     * REPLACE on content with single quotes.
     */
    public function testReplaceWithQuotes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT REPLACE(content, 'test', 'success') AS replaced
             FROM sl_str_messages
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
             FROM sl_str_messages
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
            "SELECT id FROM sl_str_messages WHERE sender = 'O''Brien'"
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
             FROM sl_str_messages
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
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_str_messages")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
