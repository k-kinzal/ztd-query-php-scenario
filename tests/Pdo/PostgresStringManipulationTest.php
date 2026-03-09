<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests string functions through the ZTD CTE rewriter on PostgreSQL via PDO.
 * Covers CONCAT, UPPER, LOWER, LENGTH, TRIM, SUBSTRING, and REPLACE.
 * @spec SPEC-10.2.93
 */
class PostgresStringManipulationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sm_contacts (
            id INTEGER PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            phone VARCHAR(30),
            notes VARCHAR(500)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_sm_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sm_contacts VALUES (1, 'Alice',   'Smith',    'alice@example.com',   '555-123-4567', 'VIP customer')");
        $this->pdo->exec("INSERT INTO pg_sm_contacts VALUES (2, 'Bob',     'Johnson',  'bob.j@example.com',   '555-234-5678', 'Prefers email')");
        $this->pdo->exec("INSERT INTO pg_sm_contacts VALUES (3, 'Charlie', 'Williams', 'charlie@test.org',    '555-345-6789', 'New lead')");
        $this->pdo->exec("INSERT INTO pg_sm_contacts VALUES (4, 'Diana',   'Brown',    'diana.b@company.net', '555-456-7890', 'Follow up needed')");
        $this->pdo->exec("INSERT INTO pg_sm_contacts VALUES (5, 'Eve',     'Davis',    'eve@test.org',        '555-567-8901', 'Inactive')");
    }

    /**
     * @spec SPEC-10.2.93
     */
    public function testConcatFullName(): void
    {
        $rows = $this->ztdQuery(
            "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM pg_sm_contacts ORDER BY last_name"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Diana Brown', $rows[0]['full_name']);
        $this->assertSame('Eve Davis', $rows[1]['full_name']);
        $this->assertSame('Bob Johnson', $rows[2]['full_name']);
        $this->assertSame('Alice Smith', $rows[3]['full_name']);
        $this->assertSame('Charlie Williams', $rows[4]['full_name']);
    }

    /**
     * @spec SPEC-10.2.93
     */
    public function testUpperLower(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT UPPER(first_name) AS upper_first, LOWER(last_name) AS lower_last FROM pg_sm_contacts WHERE id = ?",
            [1]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('ALICE', $rows[0]['upper_first']);
        $this->assertSame('smith', $rows[0]['lower_last']);
    }

    /**
     * @spec SPEC-10.2.93
     */
    public function testLengthFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT first_name, LENGTH(email) AS email_len FROM pg_sm_contacts ORDER BY email_len DESC"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Diana', $rows[0]['first_name']);
        $this->assertEquals(19, (int) $rows[0]['email_len']);
        $this->assertSame('Eve', $rows[4]['first_name']);
        $this->assertEquals(12, (int) $rows[4]['email_len']);
    }

    /**
     * @spec SPEC-10.2.93
     */
    public function testTrimFunction(): void
    {
        $this->pdo->exec("INSERT INTO pg_sm_contacts VALUES (6, '  Frank  ', 'Miller', 'frank@test.com', '555-678-9012', 'Trimmed')");

        $rows = $this->ztdQuery(
            "SELECT TRIM(first_name) AS trimmed_name FROM pg_sm_contacts WHERE id = 6"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Frank', $rows[0]['trimmed_name']);
    }

    /**
     * @spec SPEC-10.2.93
     */
    public function testSubstringExtraction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT first_name, SUBSTRING(email, 1, 5) AS email_prefix FROM pg_sm_contacts ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('alice', $rows[0]['email_prefix']);
        $this->assertSame('bob.j', $rows[1]['email_prefix']);
        $this->assertSame('charl', $rows[2]['email_prefix']);
        $this->assertSame('diana', $rows[3]['email_prefix']);
        $this->assertSame('eve@t', $rows[4]['email_prefix']);
    }

    /**
     * @spec SPEC-10.2.93
     */
    public function testReplaceFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT REPLACE(phone, '-', '') AS clean_phone FROM pg_sm_contacts ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('5551234567', $rows[0]['clean_phone']);
        $this->assertSame('5552345678', $rows[1]['clean_phone']);
        $this->assertSame('5553456789', $rows[2]['clean_phone']);
        $this->assertSame('5554567890', $rows[3]['clean_phone']);
        $this->assertSame('5555678901', $rows[4]['clean_phone']);
    }

    /**
     * @spec SPEC-10.2.93
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sm_contacts VALUES (6, 'Frank', 'Miller', 'frank@test.com', '555-678-9012', 'New')");
        $this->pdo->exec("UPDATE pg_sm_contacts SET first_name = 'ALICE_UPDATED' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_sm_contacts");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_sm_contacts')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
