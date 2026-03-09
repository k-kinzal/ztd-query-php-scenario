<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that empty strings and NULL values are stored and retrieved distinctly.
 * Shadow store must preserve the difference between '' and NULL.
 * @spec SPEC-10.2.100
 */
class SqliteEmptyStringVsNullTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_esn_profiles (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            bio TEXT,
            website TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_esn_profiles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_esn_profiles VALUES (1, 'Alice', '', 'https://alice.dev')");
        $this->pdo->exec("INSERT INTO sl_esn_profiles VALUES (2, 'Bob', 'Developer', '')");
        $this->pdo->exec("INSERT INTO sl_esn_profiles VALUES (3, 'Charlie', NULL, NULL)");
        $this->pdo->exec("INSERT INTO sl_esn_profiles VALUES (4, 'Diana', '', NULL)");
    }

    /**
     * Empty string IS NOT NULL: rows with '' bio should appear in IS NOT NULL filter.
     */
    public function testEmptyStringNotNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM sl_esn_profiles WHERE bio IS NOT NULL ORDER BY id"
        );

        // Alice (empty string), Bob (actual value), Diana (empty string)
        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(2, (int) $rows[1]['id']);
        $this->assertEquals(4, (int) $rows[2]['id']);
    }

    /**
     * NULL IS NULL: only Charlie has NULL bio.
     */
    public function testNullNotEmptyString(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM sl_esn_profiles WHERE bio IS NULL ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['id']);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    /**
     * LENGTH(bio) should be 0 for empty string, NULL for NULL.
     */
    public function testEmptyStringLength(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name, LENGTH(bio) AS bio_len FROM sl_esn_profiles ORDER BY id"
        );

        $this->assertCount(4, $rows);
        // Alice: empty string -> LENGTH = 0
        $this->assertEquals(0, (int) $rows[0]['bio_len']);
        // Bob: 'Developer' -> LENGTH = 9
        $this->assertEquals(9, (int) $rows[1]['bio_len']);
        // Charlie: NULL -> LENGTH = NULL
        $this->assertNull($rows[2]['bio_len']);
        // Diana: empty string -> LENGTH = 0
        $this->assertEquals(0, (int) $rows[3]['bio_len']);
    }

    /**
     * COALESCE returns '' for empty string, 'fallback' for NULL.
     */
    public function testCoalesceDistinguishes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name, COALESCE(bio, 'fallback') AS bio_val FROM sl_esn_profiles ORDER BY id"
        );

        $this->assertCount(4, $rows);
        // Alice: empty string is not null, COALESCE returns ''
        $this->assertSame('', $rows[0]['bio_val']);
        // Bob: 'Developer'
        $this->assertSame('Developer', $rows[1]['bio_val']);
        // Charlie: NULL -> 'fallback'
        $this->assertSame('fallback', $rows[2]['bio_val']);
        // Diana: empty string -> ''
        $this->assertSame('', $rows[3]['bio_val']);
    }

    /**
     * Update empty string to NULL, verify IS NULL, then update back to ''.
     */
    public function testUpdateEmptyToNullAndBack(): void
    {
        // Alice bio: '' -> NULL
        $this->pdo->exec("UPDATE sl_esn_profiles SET bio = NULL WHERE id = 1");

        $rows = $this->ztdQuery("SELECT bio FROM sl_esn_profiles WHERE id = 1");
        $this->assertNull($rows[0]['bio']);

        $rows = $this->ztdQuery("SELECT id FROM sl_esn_profiles WHERE bio IS NULL ORDER BY id");
        $this->assertCount(2, $rows); // Charlie (3) and now Alice (1)
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(3, (int) $rows[1]['id']);

        // Alice bio: NULL -> ''
        $this->pdo->exec("UPDATE sl_esn_profiles SET bio = '' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT bio FROM sl_esn_profiles WHERE id = 1");
        $this->assertSame('', $rows[0]['bio']);

        $rows = $this->ztdQuery("SELECT id FROM sl_esn_profiles WHERE bio IS NULL ORDER BY id");
        $this->assertCount(1, $rows); // Only Charlie again
        $this->assertEquals(3, (int) $rows[0]['id']);
    }

    /**
     * Prepared statement with empty string parameter.
     */
    public function testPreparedWithEmptyString(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM sl_esn_profiles WHERE bio = ? ORDER BY id",
            ['']
        );

        // Alice and Diana have empty string bio
        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(4, (int) $rows[1]['id']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE sl_esn_profiles SET bio = 'Updated' WHERE id = 1");
        $this->pdo->exec("INSERT INTO sl_esn_profiles VALUES (5, 'Eve', 'Bio', 'https://eve.dev')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_esn_profiles");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_esn_profiles')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
