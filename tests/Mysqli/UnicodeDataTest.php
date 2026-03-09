<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests Unicode data handling through the ZTD CTE rewriter on MySQLi.
 * Covers storage, retrieval, filtering, and length functions with multi-byte characters
 * from German, Japanese, Spanish, Irish, and Russian scripts.
 * @spec SPEC-10.2.101
 */
class UnicodeDataTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ud_contacts (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            city VARCHAR(255),
            notes TEXT
        ) CHARACTER SET utf8mb4';
    }

    protected function getTableNames(): array
    {
        return ['mi_ud_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->set_charset('utf8mb4');

        $this->mysqli->query("INSERT INTO mi_ud_contacts VALUES (1, 'Müller',    'München',     'Straße und Übergang')");
        $this->mysqli->query("INSERT INTO mi_ud_contacts VALUES (2, '田中太郎',   '東京',         '日本語のノート')");
        $this->mysqli->query("INSERT INTO mi_ud_contacts VALUES (3, 'García',    'Bogotá',      'Dirección con ñ y tilde')");
        $this->mysqli->query("INSERT INTO mi_ud_contacts VALUES (4, 'Ó Briain',  'Baile Átha Cliath', 'Gaeilge le fada')");
        $this->mysqli->query("INSERT INTO mi_ud_contacts VALUES (5, 'Иванов',    'Москва',      'Записи на русском')");
    }

    /**
     * All Unicode data round-trips correctly through ZTD.
     * @spec SPEC-10.2.101
     */
    public function testSelectUnicodeData(): void
    {
        $rows = $this->ztdQuery("SELECT name, city, notes FROM mi_ud_contacts ORDER BY id");

        $this->assertCount(5, $rows);
        $this->assertSame('Müller', $rows[0]['name']);
        $this->assertSame('München', $rows[0]['city']);
        $this->assertSame('Straße und Übergang', $rows[0]['notes']);

        $this->assertSame('田中太郎', $rows[1]['name']);
        $this->assertSame('東京', $rows[1]['city']);
        $this->assertSame('日本語のノート', $rows[1]['notes']);

        $this->assertSame('García', $rows[2]['name']);
        $this->assertSame('Bogotá', $rows[2]['city']);

        $this->assertSame('Ó Briain', $rows[3]['name']);
        $this->assertSame('Baile Átha Cliath', $rows[3]['city']);

        $this->assertSame('Иванов', $rows[4]['name']);
        $this->assertSame('Москва', $rows[4]['city']);
    }

    /**
     * WHERE clause with Unicode string literal matches correctly.
     * @spec SPEC-10.2.101
     */
    public function testWhereWithUnicode(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM mi_ud_contacts WHERE city = 'München'");

        $this->assertCount(1, $rows);
        $this->assertSame('Müller', $rows[0]['name']);
    }

    /**
     * LIKE with Unicode pattern matches correctly.
     * @spec SPEC-10.2.101
     */
    public function testLikeWithUnicode(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mi_ud_contacts
            WHERE name LIKE '%García%'
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('García', $rows[0]['name']);
    }

    /**
     * Prepared statement with Unicode parameter value.
     * @spec SPEC-10.2.101
     */
    public function testPreparedWithUnicode(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, city FROM mi_ud_contacts WHERE city = ?",
            ['東京']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('田中太郎', $rows[0]['name']);
        $this->assertSame('東京', $rows[0]['city']);
    }

    /**
     * UPDATE with Unicode values and verify through ZTD.
     * @spec SPEC-10.2.101
     */
    public function testUpdateUnicodeData(): void
    {
        $this->mysqli->query("UPDATE mi_ud_contacts SET city = 'Санкт-Петербург' WHERE id = 5");

        $rows = $this->ztdQuery("SELECT city FROM mi_ud_contacts WHERE id = 5");

        $this->assertCount(1, $rows);
        $this->assertSame('Санкт-Петербург', $rows[0]['city']);
    }

    /**
     * CHAR_LENGTH counts characters, not bytes, for multi-byte strings.
     * @spec SPEC-10.2.101
     */
    public function testLengthWithMultiByte(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CHAR_LENGTH(name) AS name_len
            FROM mi_ud_contacts
            ORDER BY id
        ");

        $this->assertCount(5, $rows);
        // Müller = 6 characters
        $this->assertEquals(6, (int) $rows[0]['name_len']);
        // 田中太郎 = 4 characters
        $this->assertEquals(4, (int) $rows[1]['name_len']);
        // García = 6 characters
        $this->assertEquals(6, (int) $rows[2]['name_len']);
        // Ó Briain = 8 characters (including space)
        $this->assertEquals(8, (int) $rows[3]['name_len']);
        // Иванов = 6 characters
        $this->assertEquals(6, (int) $rows[4]['name_len']);
    }

    /**
     * Physical table remains empty — all mutations are in ZTD shadow store.
     * @spec SPEC-10.2.101
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ud_contacts VALUES (6, 'Ζαφείρης', 'Αθήνα', 'Ελληνικά')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ud_contacts");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ud_contacts');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
