<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-byte Unicode strings (CJK, emoji, accented characters) through ZTD shadow store.
 * CTE rewriter embeds values as string literals - multi-byte chars could break.
 * @spec SPEC-10.2.101
 */
class SqliteUnicodeDataTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ud_contacts (
            id INTEGER PRIMARY KEY,
            name TEXT,
            city TEXT,
            notes TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ud_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ud_contacts VALUES (1, 'Müller', 'München', 'Kundenbetreuer')");
        $this->pdo->exec("INSERT INTO sl_ud_contacts VALUES (2, '田中太郎', '東京', '日本語のメモ')");
        $this->pdo->exec("INSERT INTO sl_ud_contacts VALUES (3, 'García', 'São Paulo', 'Cliente importante')");
        $this->pdo->exec("INSERT INTO sl_ud_contacts VALUES (4, 'O''Brien', 'Zürich', 'Notes with accent: café résumé')");
        $this->pdo->exec("INSERT INTO sl_ud_contacts VALUES (5, 'Иванов', 'Москва', 'Русский текст')");
    }

    /**
     * Verify all Unicode rows round-trip correctly through ZTD.
     */
    public function testSelectUnicodeData(): void
    {
        $rows = $this->ztdQuery("SELECT id, name, city, notes FROM sl_ud_contacts ORDER BY id");

        $this->assertCount(5, $rows);
        $this->assertSame('Müller', $rows[0]['name']);
        $this->assertSame('München', $rows[0]['city']);
        $this->assertSame('Kundenbetreuer', $rows[0]['notes']);

        $this->assertSame('田中太郎', $rows[1]['name']);
        $this->assertSame('東京', $rows[1]['city']);
        $this->assertSame('日本語のメモ', $rows[1]['notes']);

        $this->assertSame('García', $rows[2]['name']);
        $this->assertSame('São Paulo', $rows[2]['city']);
        $this->assertSame('Cliente importante', $rows[2]['notes']);

        $this->assertSame("O'Brien", $rows[3]['name']);
        $this->assertSame('Zürich', $rows[3]['city']);
        $this->assertSame('Notes with accent: café résumé', $rows[3]['notes']);

        $this->assertSame('Иванов', $rows[4]['name']);
        $this->assertSame('Москва', $rows[4]['city']);
        $this->assertSame('Русский текст', $rows[4]['notes']);
    }

    /**
     * WHERE clause with CJK characters.
     */
    public function testWhereWithUnicode(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name, city FROM sl_ud_contacts WHERE name = '田中太郎'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
        $this->assertSame('東京', $rows[0]['city']);
    }

    /**
     * LIKE with Unicode substring (accented characters).
     */
    public function testLikeWithUnicode(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name, city FROM sl_ud_contacts WHERE city LIKE '%ünch%'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertSame('München', $rows[0]['city']);
    }

    /**
     * Prepared statement with Unicode parameter.
     */
    public function testPreparedWithUnicode(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, city FROM sl_ud_contacts WHERE name = ?",
            ['Иванов']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['id']);
        $this->assertSame('Москва', $rows[0]['city']);
    }

    /**
     * Update a value to different Unicode text.
     */
    public function testUpdateUnicodeData(): void
    {
        $this->pdo->exec("UPDATE sl_ud_contacts SET city = '大阪', notes = '更新されたメモ' WHERE id = 2");

        $rows = $this->ztdQuery("SELECT city, notes FROM sl_ud_contacts WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertSame('大阪', $rows[0]['city']);
        $this->assertSame('更新されたメモ', $rows[0]['notes']);
    }

    /**
     * LENGTH for multi-byte strings (SQLite LENGTH counts characters, not bytes).
     */
    public function testLengthWithMultiByte(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name, LENGTH(name) AS name_len FROM sl_ud_contacts ORDER BY id"
        );

        $this->assertCount(5, $rows);
        // 'Müller' = 6 characters
        $this->assertEquals(6, (int) $rows[0]['name_len']);
        // '田中太郎' = 4 characters
        $this->assertEquals(4, (int) $rows[1]['name_len']);
        // 'García' = 6 characters
        $this->assertEquals(6, (int) $rows[2]['name_len']);
        // "O'Brien" = 7 characters
        $this->assertEquals(7, (int) $rows[3]['name_len']);
        // 'Иванов' = 6 characters
        $this->assertEquals(6, (int) $rows[4]['name_len']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE sl_ud_contacts SET name = '佐藤花子' WHERE id = 2");
        $this->pdo->exec("INSERT INTO sl_ud_contacts VALUES (6, 'テスト', 'テスト市', 'テストメモ')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ud_contacts");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_ud_contacts')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
