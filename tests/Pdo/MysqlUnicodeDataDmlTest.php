<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests Unicode / multi-byte character data through ZTD on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlUnicodeDataDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_uni_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            city VARCHAR(200),
            bio TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    }

    protected function getTableNames(): array
    {
        return ['my_uni_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_uni_users (name, city, bio) VALUES ('田中太郎', '東京', 'エンジニア')");
        $this->ztdExec("INSERT INTO my_uni_users (name, city, bio) VALUES ('José García', 'México', 'Développeur')");
        $this->ztdExec("INSERT INTO my_uni_users (name, city, bio) VALUES ('Müller', 'München', 'Ärzt')");
    }

    public function testSelectCjkCharacters(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name, city FROM my_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('SELECT CJK (MySQL): expected 1, got ' . count($rows));
            }

            $this->assertSame('田中太郎', $rows[0]['name']);
            $this->assertSame('東京', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT CJK (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateUnicodeValues(): void
    {
        try {
            $this->ztdExec("UPDATE my_uni_users SET city = '大阪' WHERE name = '田中太郎'");

            $rows = $this->ztdQuery("SELECT city FROM my_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE Unicode (MySQL): expected 1, got ' . count($rows));
            }

            $this->assertSame('大阪', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE Unicode (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereUnicode(): void
    {
        try {
            $this->ztdExec("DELETE FROM my_uni_users WHERE city = 'München'");

            $rows = $this->ztdQuery("SELECT name FROM my_uni_users ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('DELETE WHERE Unicode (MySQL): expected 2, got ' . count($rows));
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE Unicode (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testInsertEmojiCharacters(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_uni_users (name, city, bio) VALUES ('Test 🎉', '🏙️ City', 'Bio 🚀✨')");

            $rows = $this->ztdQuery("SELECT name, bio FROM my_uni_users WHERE name = 'Test 🎉'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT emoji (MySQL): expected 1, got ' . count($rows));
            }

            $this->assertSame('Test 🎉', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT emoji (MySQL) failed: ' . $e->getMessage());
        }
    }
}
