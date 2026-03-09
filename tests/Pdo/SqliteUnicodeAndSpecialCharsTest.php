<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests Unicode, multibyte, and special character handling in shadow store.
 *
 * Real applications store Unicode data (CJK, emoji, accented characters,
 * RTL text). The CTE rewriter must preserve these correctly through
 * string literal quoting and CAST operations.
 * @spec SPEC-3.1
 */
class SqliteUnicodeAndSpecialCharsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE usc_data (id INT PRIMARY KEY, name TEXT, description TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['usc_data'];
    }

    /**
     * Basic ASCII with special chars.
     */
    public function testAsciiSpecialChars(): void
    {
        $this->pdo->exec("INSERT INTO usc_data VALUES (1, 'O''Brien', 'Has a single quote')");

        $rows = $this->ztdQuery('SELECT * FROM usc_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame("O'Brien", $rows[0]['name']);
    }

    /**
     * CJK characters (Chinese, Japanese, Korean).
     */
    public function testCjkCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '田中太郎', '日本語テスト']);
        $stmt->execute([2, '김철수', '한국어 테스트']);
        $stmt->execute([3, '张三', '中文测试']);

        $rows = $this->ztdQuery('SELECT * FROM usc_data ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('田中太郎', $rows[0]['name']);
        $this->assertSame('김철수', $rows[1]['name']);
        $this->assertSame('张三', $rows[2]['name']);
    }

    /**
     * Emoji characters.
     */
    public function testEmojiCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '🎉 Party', 'Celebration 🎊🎈']);
        $stmt->execute([2, '👨‍👩‍👧‍👦', 'Family emoji (ZWJ sequence)']);

        $rows = $this->ztdQuery('SELECT * FROM usc_data ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('🎉 Party', $rows[0]['name']);
        $this->assertSame('👨‍👩‍👧‍👦', $rows[1]['name']);
    }

    /**
     * Accented and diacritical characters.
     */
    public function testAccentedCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, 'José García', 'Spanish name']);
        $stmt->execute([2, 'Müller', 'German umlaut']);
        $stmt->execute([3, 'Ñoño', 'Spanish ñ']);
        $stmt->execute([4, 'Ångström', 'Swedish å']);

        $rows = $this->ztdQuery('SELECT * FROM usc_data ORDER BY id');
        $this->assertCount(4, $rows);
        $this->assertSame('José García', $rows[0]['name']);
        $this->assertSame('Müller', $rows[1]['name']);
        $this->assertSame('Ñoño', $rows[2]['name']);
        $this->assertSame('Ångström', $rows[3]['name']);
    }

    /**
     * RTL (Right-to-Left) text.
     */
    public function testRtlText(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, 'مرحبا', 'Arabic']);
        $stmt->execute([2, 'שלום', 'Hebrew']);

        $rows = $this->ztdQuery('SELECT * FROM usc_data ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('مرحبا', $rows[0]['name']);
        $this->assertSame('שלום', $rows[1]['name']);
    }

    /**
     * Newlines and tabs in text data.
     */
    public function testNewlinesAndTabs(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, "Line1\nLine2", "Tab\there"]);

        $rows = $this->ztdQuery('SELECT * FROM usc_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame("Line1\nLine2", $rows[0]['name']);
        $this->assertSame("Tab\there", $rows[0]['description']);
    }

    /**
     * UPDATE with Unicode data (exec INSERT to avoid Issue #23).
     */
    public function testUpdateWithUnicode(): void
    {
        $this->pdo->exec("INSERT INTO usc_data VALUES (1, 'Original', 'English')");

        $update = $this->pdo->prepare('UPDATE usc_data SET name = ?, description = ? WHERE id = ?');
        $update->execute(['更新済み', '日本語に変更', 1]);

        $rows = $this->ztdQuery('SELECT * FROM usc_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('更新済み', $rows[0]['name']);
    }

    /**
     * WHERE clause with Unicode comparison.
     */
    public function testWhereWithUnicode(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, 'café', 'French']);
        $stmt->execute([2, 'cafe', 'English']);

        $rows = $this->ztdPrepareAndExecute(
            'SELECT * FROM usc_data WHERE name = ?',
            ['café']
        );
        $this->assertCount(1, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
    }

    /**
     * LIKE with Unicode pattern.
     */
    public function testLikeWithUnicode(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '東京タワー', 'Tokyo Tower']);
        $stmt->execute([2, '東京スカイツリー', 'Tokyo Skytree']);
        $stmt->execute([3, '大阪城', 'Osaka Castle']);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT * FROM usc_data WHERE name LIKE ?",
            ['東京%']
        );
        $this->assertCount(2, $rows);
    }

    /**
     * Very long Unicode string.
     */
    public function testLongUnicodeString(): void
    {
        $longName = str_repeat('漢字', 500); // 1000 CJK characters

        $stmt = $this->pdo->prepare('INSERT INTO usc_data VALUES (?, ?, ?)');
        $stmt->execute([1, $longName, 'long CJK string']);

        $rows = $this->ztdQuery('SELECT * FROM usc_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame($longName, $rows[0]['name']);
    }
}
