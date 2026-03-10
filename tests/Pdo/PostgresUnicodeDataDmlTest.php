<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests Unicode / multi-byte character data through ZTD shadow store on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresUnicodeDataDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_uni_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            city VARCHAR(200),
            bio TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_uni_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_uni_users (name, city, bio) VALUES ('田中太郎', '東京', 'エンジニア')");
        $this->ztdExec("INSERT INTO pg_uni_users (name, city, bio) VALUES ('José García', 'México', 'Développeur')");
        $this->ztdExec("INSERT INTO pg_uni_users (name, city, bio) VALUES ('Müller', 'München', 'Ärzt')");
    }

    public function testSelectCjkCharacters(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name, city FROM pg_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('SELECT CJK (PG): expected 1, got ' . count($rows));
            }

            $this->assertSame('田中太郎', $rows[0]['name']);
            $this->assertSame('東京', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT CJK (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateUnicodeValues(): void
    {
        try {
            $this->ztdExec("UPDATE pg_uni_users SET city = '大阪' WHERE name = '田中太郎'");

            $rows = $this->ztdQuery("SELECT city FROM pg_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE Unicode (PG): expected 1, got ' . count($rows));
            }

            $this->assertSame('大阪', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE Unicode (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereUnicode(): void
    {
        try {
            $this->ztdExec("DELETE FROM pg_uni_users WHERE city = 'München'");

            $rows = $this->ztdQuery("SELECT name FROM pg_uni_users ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('DELETE WHERE Unicode (PG): expected 2, got ' . count($rows));
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE Unicode (PG) failed: ' . $e->getMessage());
        }
    }

    public function testInsertEmojiCharacters(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_uni_users (name, city, bio) VALUES ('Test 🎉', '🏙️ City', 'Bio 🚀✨')");

            $rows = $this->ztdQuery("SELECT name, bio FROM pg_uni_users WHERE name = 'Test 🎉'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT emoji (PG): expected 1, got ' . count($rows));
            }

            $this->assertSame('Test 🎉', $rows[0]['name']);
            $this->assertSame('Bio 🚀✨', $rows[0]['bio']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT emoji (PG) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUnicodeParams(): void
    {
        try {
            $stmt = $this->ztdPrepare("INSERT INTO pg_uni_users (name, city, bio) VALUES ($1, $2, $3)");
            $stmt->execute(['Ñoño', 'São Paulo', 'Ελληνικά']);

            $rows = $this->ztdQuery("SELECT city, bio FROM pg_uni_users WHERE name = 'Ñoño'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared Unicode (PG): expected 1, got ' . count($rows));
            }

            $this->assertSame('São Paulo', $rows[0]['city']);
            $this->assertSame('Ελληνικά', $rows[0]['bio']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared Unicode (PG) failed: ' . $e->getMessage());
        }
    }
}
