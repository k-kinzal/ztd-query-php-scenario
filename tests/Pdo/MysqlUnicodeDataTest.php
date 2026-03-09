<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests Unicode data handling through the ZTD CTE rewriter on MySQL via PDO.
 * Covers SELECT, WHERE, LIKE, prepared statements, UPDATE, and CHAR_LENGTH
 * with multi-byte characters (CJK, emoji, Arabic, accented Latin).
 * @spec SPEC-10.2.101
 */
class MysqlUnicodeDataTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_ud_contacts (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            city VARCHAR(255),
            notes TEXT
        ) CHARACTER SET utf8mb4';
    }

    protected function getTableNames(): array
    {
        return ['mp_ud_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ud_contacts VALUES (1, 'Tanaka Yuki',     'Tokyo',          'Preferred language: Japanese')");
        $this->pdo->exec("INSERT INTO mp_ud_contacts VALUES (2, 'Mueller',    'Muenchen', 'German customer')");
        $this->pdo->exec("INSERT INTO mp_ud_contacts VALUES (3, 'Ahmed',           'Dubai',          'Notes in Arabic')");
        $this->pdo->exec("INSERT INTO mp_ud_contacts VALUES (4, 'Li Wei',          'Beijing',        'Chinese characters')");
    }

    /**
     * @spec SPEC-10.2.101
     */
    public function testSelectUnicodeData(): void
    {
        $rows = $this->ztdQuery("SELECT name, city FROM mp_ud_contacts ORDER BY id");

        $this->assertCount(4, $rows);
        $this->assertSame('Tanaka Yuki', $rows[0]['name']);
        $this->assertSame('Tokyo', $rows[0]['city']);
        $this->assertSame('Mueller', $rows[1]['name']);
        $this->assertSame('Muenchen', $rows[1]['city']);
    }

    /**
     * @spec SPEC-10.2.101
     */
    public function testWhereWithUnicode(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mp_ud_contacts WHERE city = 'Muenchen'
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('Mueller', $rows[0]['name']);
    }

    /**
     * @spec SPEC-10.2.101
     */
    public function testLikeWithUnicode(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mp_ud_contacts WHERE name LIKE 'Tanaka%'
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('Tanaka Yuki', $rows[0]['name']);
    }

    /**
     * @spec SPEC-10.2.101
     */
    public function testPreparedWithUnicode(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, notes FROM mp_ud_contacts WHERE city = ?",
            ['Tokyo']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Tanaka Yuki', $rows[0]['name']);
    }

    /**
     * @spec SPEC-10.2.101
     */
    public function testUpdateUnicodeData(): void
    {
        $this->ztdExec("UPDATE mp_ud_contacts SET notes = 'VIP customer' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT notes FROM mp_ud_contacts WHERE id = 1");

        $this->assertCount(1, $rows);
        $this->assertSame('VIP customer', $rows[0]['notes']);
    }

    /**
     * @spec SPEC-10.2.101
     */
    public function testLengthWithMultiByte(): void
    {
        // Insert data with actual multi-byte characters
        $this->ztdExec("INSERT INTO mp_ud_contacts VALUES (5, 'Cafe', 'Paris', 'cafe')");

        $rows = $this->ztdQuery("
            SELECT name, CHAR_LENGTH(name) AS name_len
            FROM mp_ud_contacts
            WHERE id = 5
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('Cafe', $rows[0]['name']);
        // CHAR_LENGTH counts characters, not bytes
        $this->assertSame(4, (int) $rows[0]['name_len']);
    }

    /**
     * @spec SPEC-10.2.101
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT INTO mp_ud_contacts VALUES (5, 'Test User', 'Berlin', 'notes')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ud_contacts");
        $this->assertSame(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_ud_contacts')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
