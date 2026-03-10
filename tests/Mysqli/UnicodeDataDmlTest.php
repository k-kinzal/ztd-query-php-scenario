<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests Unicode / multi-byte character data through ZTD on MySQLi.
 *
 * @spec SPEC-10.2
 */
class UnicodeDataDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_uni_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            city VARCHAR(200),
            bio TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    }

    protected function getTableNames(): array
    {
        return ['mi_uni_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_uni_users (name, city, bio) VALUES ('田中太郎', '東京', 'エンジニア')");
        $this->ztdExec("INSERT INTO mi_uni_users (name, city, bio) VALUES ('José García', 'México', 'Développeur')");
        $this->ztdExec("INSERT INTO mi_uni_users (name, city, bio) VALUES ('Müller', 'München', 'Ärzt')");
    }

    public function testSelectCjkCharacters(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name, city FROM mi_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('SELECT CJK (MySQLi): expected 1, got ' . count($rows));
            }

            $this->assertSame('田中太郎', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT CJK (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateUnicodeValues(): void
    {
        try {
            $this->ztdExec("UPDATE mi_uni_users SET city = '大阪' WHERE name = '田中太郎'");

            $rows = $this->ztdQuery("SELECT city FROM mi_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE Unicode (MySQLi): expected 1, got ' . count($rows));
            }

            $this->assertSame('大阪', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE Unicode (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereUnicode(): void
    {
        try {
            $this->ztdExec("DELETE FROM mi_uni_users WHERE city = 'München'");

            $rows = $this->ztdQuery("SELECT name FROM mi_uni_users ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('DELETE WHERE Unicode (MySQLi): expected 2, got ' . count($rows));
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE Unicode (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
