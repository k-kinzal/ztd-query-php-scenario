<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests Unicode, multibyte, and special character handling in shadow store on PostgreSQL.
 *
 * PostgreSQL has excellent Unicode support. The CTE rewriter must preserve
 * multibyte characters correctly through CAST operations.
 * @spec SPEC-3.1
 */
class PostgresUnicodeAndSpecialCharsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pusc_data (id INT PRIMARY KEY, name TEXT, description TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pusc_data'];
    }

    /**
     * CJK characters.
     */
    public function testCjkCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pusc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '田中太郎', '日本語テスト']);
        $stmt->execute([2, '김철수', '한국어 테스트']);
        $stmt->execute([3, '张三', '中文测试']);

        $rows = $this->ztdQuery('SELECT * FROM pusc_data ORDER BY id');
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
        $stmt = $this->pdo->prepare('INSERT INTO pusc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '🎉 Party', 'Celebration 🎊🎈']);

        $rows = $this->ztdQuery('SELECT * FROM pusc_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('🎉 Party', $rows[0]['name']);
    }

    /**
     * Accented characters.
     */
    public function testAccentedCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pusc_data VALUES (?, ?, ?)');
        $stmt->execute([1, 'José García', 'Spanish']);
        $stmt->execute([2, 'Müller', 'German']);

        $rows = $this->ztdQuery('SELECT * FROM pusc_data ORDER BY id');
        $this->assertSame('José García', $rows[0]['name']);
        $this->assertSame('Müller', $rows[1]['name']);
    }

    /**
     * RTL text.
     */
    public function testRtlText(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pusc_data VALUES (?, ?, ?)');
        $stmt->execute([1, 'مرحبا', 'Arabic']);

        $rows = $this->ztdQuery('SELECT * FROM pusc_data WHERE id = 1');
        $this->assertSame('مرحبا', $rows[0]['name']);
    }

    /**
     * Unicode WHERE comparison.
     */
    public function testWhereUnicodeComparison(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pusc_data VALUES (?, ?, ?)');
        $stmt->execute([1, 'café', 'French']);
        $stmt->execute([2, 'cafe', 'English']);

        $rows = $this->ztdPrepareAndExecute(
            'SELECT * FROM pusc_data WHERE name = ?',
            ['café']
        );
        $this->assertCount(1, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
    }

    /**
     * LIKE with Unicode.
     */
    public function testLikeUnicode(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pusc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '東京タワー', 'Tokyo Tower']);
        $stmt->execute([2, '東京スカイツリー', 'Tokyo Skytree']);
        $stmt->execute([3, '大阪城', 'Osaka Castle']);

        $rows = $this->ztdPrepareAndExecute(
            'SELECT * FROM pusc_data WHERE name LIKE ?',
            ['東京%']
        );
        $this->assertCount(2, $rows);
    }

    /**
     * Newlines in text data.
     */
    public function testNewlines(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pusc_data VALUES (?, ?, ?)');
        $stmt->execute([1, "Line1\nLine2", "Tab\there"]);

        $rows = $this->ztdQuery('SELECT * FROM pusc_data WHERE id = 1');
        $this->assertSame("Line1\nLine2", $rows[0]['name']);
        $this->assertSame("Tab\there", $rows[0]['description']);
    }

    /**
     * Table name in string literal (not affected on PostgreSQL per Issue #67 SQLite-only).
     */
    public function testTableNameInStringLiteral(): void
    {
        $this->pdo->exec("INSERT INTO pusc_data VALUES (1, 'test', 'from pusc_data table')");

        $rows = $this->ztdQuery(
            "SELECT id, 'from pusc_data table' AS source FROM pusc_data WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('from pusc_data table', $rows[0]['source']);
    }
}
