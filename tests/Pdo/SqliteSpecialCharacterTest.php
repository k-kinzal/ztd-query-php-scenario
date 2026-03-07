<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests that the shadow store correctly handles special characters,
 * Unicode, and edge-case string values in CTE-rewritten queries.
 */
class SqliteSpecialCharacterTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE char_test (id INTEGER PRIMARY KEY, val TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testSingleQuoteInValue(): void
    {
        $this->pdo->exec("INSERT INTO char_test (id, val) VALUES (1, 'it''s a test')");

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame("it's a test", $rows[0]['val']);
    }

    public function testDoubleQuoteInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, 'say "hello"']);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('say "hello"', $rows[0]['val']);
    }

    public function testBackslashInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, 'path\\to\\file']);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('path\\to\\file', $rows[0]['val']);
    }

    public function testNewlineInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, "line1\nline2"]);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame("line1\nline2", $rows[0]['val']);
    }

    public function testUnicodeInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, 'こんにちは世界']);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('こんにちは世界', $rows[0]['val']);
    }

    public function testEmojiInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, '🎉🚀']);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('🎉🚀', $rows[0]['val']);
    }

    public function testEmptyStringInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, '']);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('', $rows[0]['val']);
    }

    public function testTabAndCarriageReturnInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, "col1\tcol2\r\n"]);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame("col1\tcol2\r\n", $rows[0]['val']);
    }

    public function testSqlKeywordInValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, "SELECT * FROM users; DROP TABLE users;--"]);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame("SELECT * FROM users; DROP TABLE users;--", $rows[0]['val']);
    }

    public function testLikeWithSpecialCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO char_test (id, val) VALUES (?, ?)');
        $stmt->execute([1, '100% done']);
        $stmt->execute([2, 'under_score']);
        $stmt->execute([3, 'normal']);

        // LIKE '%' and '_' are wildcards
        $stmt = $this->pdo->query("SELECT * FROM char_test WHERE val LIKE '%done'");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('100% done', $rows[0]['val']);
    }

    public function testUpdateWithSpecialCharacters(): void
    {
        $this->pdo->exec("INSERT INTO char_test (id, val) VALUES (1, 'original')");

        $stmt = $this->pdo->prepare('UPDATE char_test SET val = ? WHERE id = ?');
        $stmt->execute(["it's \"quoted\" with\\backslash", 1]);

        $stmt = $this->pdo->query('SELECT val FROM char_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame("it's \"quoted\" with\\backslash", $rows[0]['val']);
    }
}
