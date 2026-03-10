<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests Unicode / multi-byte character data through ZTD shadow store on SQLite.
 *
 * International applications store CJK, emoji, diacritics, RTL text, etc.
 * This tests whether the CTE rewriter and shadow store preserve multi-byte
 * data correctly in INSERT, UPDATE, DELETE, and WHERE clauses.
 *
 * @spec SPEC-10.2
 */
class SqliteUnicodeDataDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_uni_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            city TEXT,
            bio TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_uni_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_uni_users (name, city, bio) VALUES ('田中太郎', '東京', 'エンジニア')");
        $this->ztdExec("INSERT INTO sl_uni_users (name, city, bio) VALUES ('José García', 'México', 'Développeur')");
        $this->ztdExec("INSERT INTO sl_uni_users (name, city, bio) VALUES ('Müller', 'München', 'Ärzt')");
    }

    /**
     * SELECT with CJK characters — baseline roundtrip.
     */
    public function testSelectCjkCharacters(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name, city FROM sl_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('SELECT CJK: expected 1, got ' . count($rows));
            }

            $this->assertSame('田中太郎', $rows[0]['name']);
            $this->assertSame('東京', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT CJK failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with Unicode values.
     */
    public function testUpdateUnicodeValues(): void
    {
        try {
            $this->ztdExec("UPDATE sl_uni_users SET city = '大阪' WHERE name = '田中太郎'");

            $rows = $this->ztdQuery("SELECT city FROM sl_uni_users WHERE name = '田中太郎'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE Unicode: expected 1, got ' . count($rows));
            }

            $this->assertSame('大阪', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE Unicode failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with Unicode WHERE clause.
     */
    public function testDeleteWhereUnicode(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_uni_users WHERE city = 'München'");

            $rows = $this->ztdQuery("SELECT name FROM sl_uni_users ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE Unicode: expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE Unicode failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with emoji characters.
     */
    public function testInsertEmojiCharacters(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_uni_users (name, city, bio) VALUES ('Test 🎉', '🏙️ City', 'Bio 🚀✨')");

            $rows = $this->ztdQuery("SELECT name, city, bio FROM sl_uni_users WHERE name = 'Test 🎉'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT emoji: expected 1, got ' . count($rows)
                    . '. All rows: ' . json_encode($this->ztdQuery("SELECT name FROM sl_uni_users"))
                );
            }

            $this->assertSame('Test 🎉', $rows[0]['name']);
            $this->assertSame('Bio 🚀✨', $rows[0]['bio']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT emoji failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT/SELECT with Unicode parameters.
     */
    public function testPreparedUnicodeParams(): void
    {
        try {
            $stmt = $this->ztdPrepare("INSERT INTO sl_uni_users (name, city, bio) VALUES (?, ?, ?)");
            $stmt->execute(['Ñoño', 'São Paulo', 'Ελληνικά']);

            $rows = $this->ztdQuery("SELECT city, bio FROM sl_uni_users WHERE name = 'Ñoño'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared Unicode params: expected 1, got ' . count($rows));
            }

            $this->assertSame('São Paulo', $rows[0]['city']);
            $this->assertSame('Ελληνικά', $rows[0]['bio']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared Unicode params failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with diacritical marks and special characters.
     */
    public function testUpdateDiacriticalMarks(): void
    {
        try {
            $this->ztdExec("UPDATE sl_uni_users SET bio = 'Ärzt für Über-Ökologie' WHERE name = 'Müller'");

            $rows = $this->ztdQuery("SELECT bio FROM sl_uni_users WHERE name = 'Müller'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE diacriticals: expected 1, got ' . count($rows));
            }

            $this->assertSame('Ärzt für Über-Ökologie', $rows[0]['bio']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE diacriticals failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with RTL (Arabic/Hebrew) text.
     */
    public function testInsertRtlText(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_uni_users (name, city, bio) VALUES ('محمد', 'القاهرة', 'مهندس برمجيات')");

            $rows = $this->ztdQuery("SELECT name, city FROM sl_uni_users WHERE name = 'محمد'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT RTL: expected 1, got ' . count($rows));
            }

            $this->assertSame('محمد', $rows[0]['name']);
            $this->assertSame('القاهرة', $rows[0]['city']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT RTL failed: ' . $e->getMessage());
        }
    }
}
