<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests Unicode, multibyte, and special character handling in shadow store on MySQL.
 * @spec SPEC-3.1
 */
class MysqlUnicodeAndSpecialCharsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE musc_data (id INT PRIMARY KEY, name TEXT CHARACTER SET utf8mb4, description TEXT CHARACTER SET utf8mb4)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['musc_data'];
    }

    /**
     * CJK characters.
     */
    public function testCjkCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO musc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '田中太郎', '日本語テスト']);
        $stmt->execute([2, '김철수', '한국어 테스트']);
        $stmt->execute([3, '张三', '中文测试']);

        $rows = $this->ztdQuery('SELECT * FROM musc_data ORDER BY id');
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
        $stmt = $this->pdo->prepare('INSERT INTO musc_data VALUES (?, ?, ?)');
        $stmt->execute([1, '🎉 Party', 'Celebration 🎊🎈']);

        $rows = $this->ztdQuery('SELECT * FROM musc_data WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('🎉 Party', $rows[0]['name']);
    }

    /**
     * Accented characters.
     */
    public function testAccentedCharacters(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO musc_data VALUES (?, ?, ?)');
        $stmt->execute([1, 'José García', 'Spanish']);
        $stmt->execute([2, 'Müller', 'German']);

        $rows = $this->ztdQuery('SELECT * FROM musc_data ORDER BY id');
        $this->assertSame('José García', $rows[0]['name']);
        $this->assertSame('Müller', $rows[1]['name']);
    }

    /**
     * Newlines in text data.
     */
    public function testNewlines(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO musc_data VALUES (?, ?, ?)');
        $stmt->execute([1, "Line1\nLine2", "Tab\there"]);

        $rows = $this->ztdQuery('SELECT * FROM musc_data WHERE id = 1');
        $this->assertSame("Line1\nLine2", $rows[0]['name']);
        $this->assertSame("Tab\there", $rows[0]['description']);
    }

    /**
     * Table name in string literal (not affected on MySQL per Issue #67 SQLite-only).
     */
    public function testTableNameInStringLiteral(): void
    {
        $this->pdo->exec("INSERT INTO musc_data VALUES (1, 'test', 'from musc_data table')");

        $rows = $this->ztdQuery(
            "SELECT id, 'from musc_data table' AS source FROM musc_data WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('from musc_data table', $rows[0]['source']);
    }
}
