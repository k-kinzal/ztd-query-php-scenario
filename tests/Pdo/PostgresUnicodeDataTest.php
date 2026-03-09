<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests Unicode data handling through ZTD on PostgreSQL via PDO.
 * PostgreSQL uses UTF-8 by default, so no special charset configuration is needed.
 * Covers CJK, emoji, Arabic, accented Latin, and multi-byte character operations.
 * @spec SPEC-10.2.101
 */
class PostgresUnicodeDataTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ud_contacts (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            city VARCHAR(255),
            notes TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_ud_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ud_contacts VALUES (1, '田中太郎', '東京', 'Japanese contact')");
        $this->pdo->exec("INSERT INTO pg_ud_contacts VALUES (2, 'José García', 'São Paulo', 'Brazilian office')");
        $this->pdo->exec("INSERT INTO pg_ud_contacts VALUES (3, 'محمد أحمد', 'القاهرة', 'Cairo branch')");
        $this->pdo->exec("INSERT INTO pg_ud_contacts VALUES (4, 'Müller', 'Zürich', 'Swiss partner')");
        $this->pdo->exec("INSERT INTO pg_ud_contacts VALUES (5, '김민수', '서울', 'Korean office')");
    }

    /**
     * Select and verify Unicode data is stored and retrieved correctly.
     */
    public function testSelectUnicodeData(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, city, notes FROM pg_ud_contacts ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('田中太郎', $rows[0]['name']);
        $this->assertSame('東京', $rows[0]['city']);
        $this->assertSame('José García', $rows[1]['name']);
        $this->assertSame('São Paulo', $rows[1]['city']);
        $this->assertSame('محمد أحمد', $rows[2]['name']);
        $this->assertSame('القاهرة', $rows[2]['city']);
        $this->assertSame('Müller', $rows[3]['name']);
        $this->assertSame('Zürich', $rows[3]['city']);
        $this->assertSame('김민수', $rows[4]['name']);
        $this->assertSame('서울', $rows[4]['city']);
    }

    /**
     * WHERE clause with Unicode literal.
     */
    public function testWhereWithUnicode(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, city FROM pg_ud_contacts WHERE city = '東京'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('田中太郎', $rows[0]['name']);
        $this->assertSame('東京', $rows[0]['city']);
    }

    /**
     * LIKE and ILIKE with Unicode patterns.
     * ILIKE is PostgreSQL-specific case-insensitive LIKE.
     */
    public function testLikeWithUnicode(): void
    {
        // Standard LIKE with Unicode
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_ud_contacts WHERE name LIKE '%García%'"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('José García', $rows[0]['name']);

        // PostgreSQL ILIKE (case-insensitive)
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_ud_contacts WHERE name ILIKE '%müller%'"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Müller', $rows[0]['name']);
    }

    /**
     * Prepared statement with Unicode parameter values.
     */
    public function testPreparedWithUnicode(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM pg_ud_contacts WHERE city = ?",
            ['서울']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['id']);
        $this->assertSame('김민수', $rows[0]['name']);
    }

    /**
     * Update with Unicode data.
     */
    public function testUpdateUnicodeData(): void
    {
        $this->pdo->exec("UPDATE pg_ud_contacts SET city = '大阪', notes = '転勤しました' WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT city, notes FROM pg_ud_contacts WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('大阪', $rows[0]['city']);
        $this->assertSame('転勤しました', $rows[0]['notes']);
    }

    /**
     * CHAR_LENGTH with multi-byte characters counts characters, not bytes.
     */
    public function testLengthWithMultiByte(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, CHAR_LENGTH(name) AS name_len
             FROM pg_ud_contacts
             ORDER BY id"
        );

        $this->assertCount(5, $rows);
        // '田中太郎' = 4 characters
        $this->assertSame('田中太郎', $rows[0]['name']);
        $this->assertEquals(4, (int) $rows[0]['name_len']);
        // 'José García' = 11 characters (including accented chars as single chars)
        $this->assertSame('José García', $rows[1]['name']);
        $this->assertEquals(11, (int) $rows[1]['name_len']);
        // 'محمد أحمد' = 9 characters (including space)
        $this->assertSame('محمد أحمد', $rows[2]['name']);
        $this->assertEquals(9, (int) $rows[2]['name_len']);
        // 'Müller' = 6 characters
        $this->assertSame('Müller', $rows[3]['name']);
        $this->assertEquals(6, (int) $rows[3]['name_len']);
        // '김민수' = 3 characters
        $this->assertSame('김민수', $rows[4]['name']);
        $this->assertEquals(3, (int) $rows[4]['name_len']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_ud_contacts VALUES (6, 'Ñoño', 'España', 'Spanish test')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_ud_contacts");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_ud_contacts')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
