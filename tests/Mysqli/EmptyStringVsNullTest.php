<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that the ZTD CTE rewriter correctly distinguishes between empty strings
 * and NULL values on MySQLi. MySQL treats '' and NULL as distinct values, and ZTD
 * must preserve this distinction through the shadow store.
 * @spec SPEC-10.2.100
 */
class EmptyStringVsNullTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_esn_profiles (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            bio TEXT,
            website VARCHAR(500)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_esn_profiles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_esn_profiles VALUES (1, 'Alice', 'Developer and writer', 'https://alice.dev')");
        $this->mysqli->query("INSERT INTO mi_esn_profiles VALUES (2, 'Bob',   '',                      NULL)");
        $this->mysqli->query("INSERT INTO mi_esn_profiles VALUES (3, 'Charlie', NULL,                  '')");
        $this->mysqli->query("INSERT INTO mi_esn_profiles VALUES (4, 'Diana',   '',                    '')");
    }

    /**
     * Empty string is not NULL: IS NOT NULL should match empty strings.
     * @spec SPEC-10.2.100
     */
    public function testEmptyStringNotNull(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mi_esn_profiles
            WHERE bio IS NOT NULL
            ORDER BY id
        ");

        // Alice has a real bio, Bob has '', Diana has '' => 3 rows
        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Diana', $names);
    }

    /**
     * NULL is not empty string: IS NULL should not match empty strings.
     * @spec SPEC-10.2.100
     */
    public function testNullNotEmptyString(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mi_esn_profiles
            WHERE bio IS NULL
            ORDER BY id
        ");

        // Only Charlie has NULL bio
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    /**
     * CHAR_LENGTH of empty string is 0, CHAR_LENGTH of NULL is NULL.
     * @spec SPEC-10.2.100
     */
    public function testEmptyStringLength(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CHAR_LENGTH(bio) AS bio_len
            FROM mi_esn_profiles
            ORDER BY id
        ");

        $this->assertCount(4, $rows);
        // Alice: 'Developer and writer' = 20
        $this->assertEquals(20, (int) $rows[0]['bio_len']);
        // Bob: '' = 0
        $this->assertEquals(0, (int) $rows[1]['bio_len']);
        // Charlie: NULL => NULL
        $this->assertNull($rows[2]['bio_len']);
        // Diana: '' = 0
        $this->assertEquals(0, (int) $rows[3]['bio_len']);
    }

    /**
     * COALESCE distinguishes between NULL (uses fallback) and '' (keeps '').
     * @spec SPEC-10.2.100
     */
    public function testCoalesceDistinguishes(): void
    {
        $rows = $this->ztdQuery("
            SELECT name,
                   COALESCE(bio, 'no bio') AS bio_display,
                   COALESCE(website, 'no site') AS site_display
            FROM mi_esn_profiles
            ORDER BY id
        ");

        $this->assertCount(4, $rows);
        // Alice: bio='Developer...', website='https://...'
        $this->assertSame('Developer and writer', $rows[0]['bio_display']);
        $this->assertSame('https://alice.dev', $rows[0]['site_display']);
        // Bob: bio='', website=NULL
        $this->assertSame('', $rows[1]['bio_display']);
        $this->assertSame('no site', $rows[1]['site_display']);
        // Charlie: bio=NULL, website=''
        $this->assertSame('no bio', $rows[2]['bio_display']);
        $this->assertSame('', $rows[2]['site_display']);
        // Diana: bio='', website=''
        $this->assertSame('', $rows[3]['bio_display']);
        $this->assertSame('', $rows[3]['site_display']);
    }

    /**
     * UPDATE from empty string to NULL and back.
     * @spec SPEC-10.2.100
     */
    public function testUpdateEmptyToNullAndBack(): void
    {
        // Change Bob's bio from '' to NULL
        $this->mysqli->query("UPDATE mi_esn_profiles SET bio = NULL WHERE id = 2");

        $rows = $this->ztdQuery("SELECT bio FROM mi_esn_profiles WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['bio']);

        // Change it back from NULL to ''
        $this->mysqli->query("UPDATE mi_esn_profiles SET bio = '' WHERE id = 2");

        $rows2 = $this->ztdQuery("SELECT bio FROM mi_esn_profiles WHERE id = 2");
        $this->assertCount(1, $rows2);
        $this->assertSame('', $rows2[0]['bio']);
    }

    /**
     * Prepared statement with empty string as a bound value.
     * @spec SPEC-10.2.100
     */
    public function testPreparedWithEmptyString(): void
    {
        $stmt = $this->mysqli->prepare("INSERT INTO mi_esn_profiles (id, name, bio, website) VALUES (?, ?, ?, ?)");
        $id = 5;
        $name = 'Eve';
        $bio = '';
        $website = '';
        $stmt->bind_param('isss', $id, $name, $bio, $website);
        $stmt->execute();

        $rows = $this->ztdQuery("SELECT bio, website FROM mi_esn_profiles WHERE id = 5");
        $this->assertCount(1, $rows);
        $this->assertSame('', $rows[0]['bio']);
        $this->assertSame('', $rows[0]['website']);

        // Confirm it is NOT NULL
        $rows2 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_esn_profiles WHERE id = 5 AND bio IS NOT NULL AND website IS NOT NULL");
        $this->assertEquals(1, (int) $rows2[0]['cnt']);
    }

    /**
     * Physical table remains empty — all mutations are in ZTD shadow store.
     * @spec SPEC-10.2.100
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_esn_profiles SET bio = 'updated' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT bio FROM mi_esn_profiles WHERE id = 1");
        $this->assertSame('updated', $rows[0]['bio']);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_esn_profiles');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
