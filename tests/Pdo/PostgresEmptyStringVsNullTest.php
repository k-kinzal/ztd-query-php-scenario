<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests that PostgreSQL correctly distinguishes empty strings from NULL values
 * through ZTD shadow store. PostgreSQL treats '' and NULL as distinct values,
 * unlike Oracle.
 * @spec SPEC-10.2.100
 */
class PostgresEmptyStringVsNullTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_esn_profiles (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            bio TEXT,
            website VARCHAR(500)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_esn_profiles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_esn_profiles VALUES (1, 'Alice', 'Software engineer', 'https://alice.dev')");
        $this->pdo->exec("INSERT INTO pg_esn_profiles VALUES (2, 'Bob', '', NULL)");
        $this->pdo->exec("INSERT INTO pg_esn_profiles VALUES (3, 'Charlie', NULL, '')");
        $this->pdo->exec("INSERT INTO pg_esn_profiles VALUES (4, 'Diana', '', '')");
    }

    /**
     * Empty string is not NULL: IS NOT NULL should include rows with ''.
     */
    public function testEmptyStringNotNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_esn_profiles WHERE bio IS NOT NULL ORDER BY id"
        );

        // Alice has text, Bob has '', Diana has '' - all are NOT NULL
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Diana', $rows[2]['name']);
    }

    /**
     * NULL is not empty string: IS NULL should exclude rows with ''.
     */
    public function testNullNotEmptyString(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_esn_profiles WHERE bio IS NULL ORDER BY id"
        );

        // Only Charlie has NULL bio
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    /**
     * LENGTH of empty string is 0, LENGTH of NULL is NULL.
     */
    public function testEmptyStringLength(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, LENGTH(bio) AS bio_len
             FROM pg_esn_profiles
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(17, (int) $rows[0]['bio_len']);    // 'Software engineer'
        $this->assertEquals(0, (int) $rows[1]['bio_len']);     // ''
        $this->assertNull($rows[2]['bio_len']);                 // NULL
        $this->assertEquals(0, (int) $rows[3]['bio_len']);     // ''
    }

    /**
     * COALESCE distinguishes between '' and NULL.
     */
    public function testCoalesceDistinguishes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    COALESCE(bio, 'no bio') AS bio_display,
                    COALESCE(website, 'no site') AS site_display
             FROM pg_esn_profiles
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        // Alice: both have values
        $this->assertSame('Software engineer', $rows[0]['bio_display']);
        $this->assertSame('https://alice.dev', $rows[0]['site_display']);
        // Bob: bio='', website=NULL -> COALESCE picks '' for bio, 'no site' for website
        $this->assertSame('', $rows[1]['bio_display']);
        $this->assertSame('no site', $rows[1]['site_display']);
        // Charlie: bio=NULL, website='' -> COALESCE picks 'no bio' for bio, '' for website
        $this->assertSame('no bio', $rows[2]['bio_display']);
        $this->assertSame('', $rows[2]['site_display']);
        // Diana: both ''
        $this->assertSame('', $rows[3]['bio_display']);
        $this->assertSame('', $rows[3]['site_display']);
    }

    /**
     * Update empty string to NULL and back.
     */
    public function testUpdateEmptyToNullAndBack(): void
    {
        // Update Bob's bio from '' to NULL
        $this->pdo->exec("UPDATE pg_esn_profiles SET bio = NULL WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT bio FROM pg_esn_profiles WHERE id = 2"
        );
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['bio']);

        // Now update back from NULL to ''
        $this->pdo->exec("UPDATE pg_esn_profiles SET bio = '' WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT bio FROM pg_esn_profiles WHERE id = 2"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('', $rows[0]['bio']);
    }

    /**
     * Prepared statement with empty string parameter.
     */
    public function testPreparedWithEmptyString(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO pg_esn_profiles VALUES (?, ?, ?, ?)");
        $stmt->execute([5, 'Eve', '', '']);

        $rows = $this->ztdQuery(
            "SELECT name, bio, website FROM pg_esn_profiles WHERE id = 5"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertSame('', $rows[0]['bio']);
        $this->assertSame('', $rows[0]['website']);

        // Verify it shows up in IS NOT NULL queries
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_esn_profiles WHERE bio IS NOT NULL"
        );
        $this->assertEquals(4, (int) $rows[0]['cnt']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_esn_profiles VALUES (5, 'Eve', 'New profile', 'https://eve.io')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_esn_profiles");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_esn_profiles')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
