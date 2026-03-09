<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that the ZTD CTE rewriter correctly distinguishes empty strings
 * from NULLs on MySQL via PDO.
 * Covers IS NULL / IS NOT NULL with empty strings, CHAR_LENGTH, COALESCE,
 * UPDATE between empty and NULL, and prepared statements.
 * @spec SPEC-10.2.100
 */
class MysqlEmptyStringVsNullTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_esn_profiles (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            bio TEXT,
            website VARCHAR(500)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_esn_profiles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_esn_profiles VALUES (1, 'Alice',   '',     'https://alice.dev')");
        $this->pdo->exec("INSERT INTO mp_esn_profiles VALUES (2, 'Bob',     NULL,   '')");
        $this->pdo->exec("INSERT INTO mp_esn_profiles VALUES (3, 'Charlie', 'Coder','https://charlie.io')");
        $this->pdo->exec("INSERT INTO mp_esn_profiles VALUES (4, 'Diana',   '',     NULL)");
    }

    /**
     * @spec SPEC-10.2.100
     */
    public function testEmptyStringNotNull(): void
    {
        // Empty string is NOT NULL in MySQL (unlike Oracle)
        $rows = $this->ztdQuery("
            SELECT name FROM mp_esn_profiles
            WHERE bio IS NOT NULL
            ORDER BY id
        ");

        // Alice (bio=''), Charlie (bio='Coder'), Diana (bio='')
        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertContains('Diana', $names);
    }

    /**
     * @spec SPEC-10.2.100
     */
    public function testNullNotEmptyString(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mp_esn_profiles
            WHERE bio IS NULL
            ORDER BY id
        ");

        // Only Bob has bio = NULL
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    /**
     * @spec SPEC-10.2.100
     */
    public function testEmptyStringLength(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, CHAR_LENGTH(bio) AS bio_len
            FROM mp_esn_profiles
            WHERE bio IS NOT NULL
            ORDER BY id
        ");

        $this->assertCount(3, $rows);
        // Alice: empty string => length 0
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(0, (int) $rows[0]['bio_len']);
        // Charlie: 'Coder' => length 5
        $this->assertSame('Charlie', $rows[1]['name']);
        $this->assertSame(5, (int) $rows[1]['bio_len']);
        // Diana: empty string => length 0
        $this->assertSame('Diana', $rows[2]['name']);
        $this->assertSame(0, (int) $rows[2]['bio_len']);
    }

    /**
     * @spec SPEC-10.2.100
     */
    public function testCoalesceDistinguishes(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, COALESCE(bio, 'NO_BIO') AS bio_display
            FROM mp_esn_profiles
            ORDER BY id
        ");

        $this->assertCount(4, $rows);
        // Alice: bio is '' (empty), COALESCE returns '' (not the fallback)
        $this->assertSame('', $rows[0]['bio_display']);
        // Bob: bio is NULL, COALESCE returns 'NO_BIO'
        $this->assertSame('NO_BIO', $rows[1]['bio_display']);
        // Charlie: bio is 'Coder'
        $this->assertSame('Coder', $rows[2]['bio_display']);
        // Diana: bio is '' (empty), COALESCE returns ''
        $this->assertSame('', $rows[3]['bio_display']);
    }

    /**
     * @spec SPEC-10.2.100
     */
    public function testUpdateEmptyToNullAndBack(): void
    {
        // Update empty string to NULL
        $this->ztdExec("UPDATE mp_esn_profiles SET bio = NULL WHERE id = 1");

        $rows = $this->ztdQuery("SELECT bio FROM mp_esn_profiles WHERE id = 1");
        $this->assertNull($rows[0]['bio']);

        // Update NULL to empty string
        $this->ztdExec("UPDATE mp_esn_profiles SET bio = '' WHERE id = 2");

        $rows = $this->ztdQuery("SELECT bio FROM mp_esn_profiles WHERE id = 2");
        $this->assertSame('', $rows[0]['bio']);

        // After updates: id=1 bio=NULL, id=2 bio='', id=3 bio='Coder', id=4 bio=''
        // Only id=1 has bio IS NULL
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_esn_profiles WHERE bio IS NULL");
        $this->assertSame(1, (int) $rows[0]['cnt']);
    }

    /**
     * @spec SPEC-10.2.100
     */
    public function testPreparedWithEmptyString(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO mp_esn_profiles VALUES (?, ?, ?, ?)");
        $stmt->execute([5, 'Eve', '', null]);

        $rows = $this->ztdQuery("SELECT bio, website FROM mp_esn_profiles WHERE id = 5");

        $this->assertCount(1, $rows);
        $this->assertSame('', $rows[0]['bio']);
        $this->assertNull($rows[0]['website']);
    }

    /**
     * @spec SPEC-10.2.100
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT INTO mp_esn_profiles VALUES (5, 'Eve', 'tester', 'https://eve.dev')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_esn_profiles");
        $this->assertSame(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_esn_profiles')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
