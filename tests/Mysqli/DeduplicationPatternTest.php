<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests deduplication query patterns through ZTD shadow store.
 * Simulates finding and handling duplicate rows using GROUP BY, HAVING,
 * and ROW_NUMBER() window functions.
 * @spec SPEC-10.2.38
 */
class DeduplicationPatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_dedup_contacts (
                id INT PRIMARY KEY,
                email VARCHAR(255),
                name VARCHAR(255),
                phone VARCHAR(50),
                created_at DATETIME
            )',
            'CREATE TABLE mi_dedup_clean (
                id INT PRIMARY KEY,
                email VARCHAR(255),
                name VARCHAR(255),
                phone VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_dedup_clean', 'mi_dedup_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert contacts with duplicates (same email, different names/phones)
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (1, 'alice@example.com', 'Alice Smith', '555-0001', '2024-01-01 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (2, 'bob@example.com', 'Bob Jones', '555-0002', '2024-01-02 11:00:00')");
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (3, 'alice@example.com', 'Alice S.', '555-0003', '2024-01-03 12:00:00')");
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (4, 'charlie@example.com', 'Charlie Brown', '555-0004', '2024-01-04 13:00:00')");
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (5, 'bob@example.com', 'Robert Jones', '555-0005', '2024-01-05 14:00:00')");
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (6, 'alice@example.com', 'A. Smith', '555-0006', '2024-01-06 15:00:00')");
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (7, 'diana@example.com', 'Diana Prince', '555-0007', '2024-01-07 16:00:00')");
    }

    /**
     * Find duplicate emails using GROUP BY HAVING COUNT > 1.
     */
    public function testFindDuplicateEmails(): void
    {
        $rows = $this->ztdQuery(
            "SELECT email, COUNT(*) AS cnt
             FROM mi_dedup_contacts
             GROUP BY email
             HAVING COUNT(*) > 1
             ORDER BY cnt DESC, email"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('alice@example.com', $rows[0]['email']);
        $this->assertEquals(3, (int) $rows[0]['cnt']);
        $this->assertSame('bob@example.com', $rows[1]['email']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);
    }

    /**
     * Find unique emails (no duplicates).
     */
    public function testFindUniqueEmails(): void
    {
        $rows = $this->ztdQuery(
            "SELECT email, COUNT(*) AS cnt
             FROM mi_dedup_contacts
             GROUP BY email
             HAVING COUNT(*) = 1
             ORDER BY email"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('charlie@example.com', $rows[0]['email']);
        $this->assertSame('diana@example.com', $rows[1]['email']);
    }

    /**
     * ROW_NUMBER derived table returns empty (SPEC-3.3a known limitation).
     * Derived tables as sole FROM source return empty on MySQL.
     */
    public function testRowNumberDerivedTableReturnsEmpty(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, email, name
             FROM (
                 SELECT id, email, name,
                        ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) AS rn
                 FROM mi_dedup_contacts
             ) ranked
             WHERE rn = 1
             ORDER BY email"
        );

        $this->assertCount(0, $rows, 'Derived table dedup returns empty on MySQL (SPEC-3.3a)');
    }

    /**
     * Workaround: GROUP BY MIN/MAX for first/last occurrence.
     */
    public function testKeepFirstOccurrenceWorkaround(): void
    {
        $rows = $this->ztdQuery(
            "SELECT MIN(id) AS id, email, MIN(name) AS name
             FROM mi_dedup_contacts
             GROUP BY email
             ORDER BY email"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('alice@example.com', $rows[0]['email']);
    }

    /**
     * Workaround: GROUP BY MAX for last occurrence.
     */
    public function testKeepLastOccurrenceWorkaround(): void
    {
        $rows = $this->ztdQuery(
            "SELECT MAX(id) AS id, email
             FROM mi_dedup_contacts
             GROUP BY email
             ORDER BY email"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(6, (int) $rows[0]['id']); // Last Alice
        $this->assertEquals(5, (int) $rows[1]['id']); // Last Bob
    }

    /**
     * Count duplicates summary: total rows, unique emails, duplicate count.
     */
    public function testDuplicateSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS total_rows,
                    COUNT(DISTINCT email) AS unique_emails,
                    COUNT(*) - COUNT(DISTINCT email) AS duplicate_count
             FROM mi_dedup_contacts"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(7, (int) $rows[0]['total_rows']);
        $this->assertEquals(4, (int) $rows[0]['unique_emails']);
        $this->assertEquals(3, (int) $rows[0]['duplicate_count']);
    }

    /**
     * Find all duplicate rows (not just the extra copies, but ALL rows that share an email).
     */
    public function testFindAllDuplicateRows(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.email, c.name
             FROM mi_dedup_contacts c
             WHERE c.email IN (
                 SELECT email FROM mi_dedup_contacts
                 GROUP BY email HAVING COUNT(*) > 1
             )
             ORDER BY c.email, c.id"
        );

        $this->assertCount(5, $rows); // 3 Alices + 2 Bobs
        $this->assertSame('alice@example.com', $rows[0]['email']);
        $this->assertEquals(1, (int) $rows[0]['id']);
    }

    /**
     * Delete duplicates keeping the first occurrence, then verify.
     */
    public function testDeleteDuplicatesKeepFirst(): void
    {
        // Delete rows that are NOT the first occurrence per email
        $this->mysqli->query(
            "DELETE FROM mi_dedup_contacts WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) AS rn
                    FROM mi_dedup_contacts
                ) ranked WHERE rn > 1
            )"
        );

        $rows = $this->ztdQuery(
            "SELECT id, email, name FROM mi_dedup_contacts ORDER BY email"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Alice Smith', $rows[0]['name']);
        $this->assertSame('Bob Jones', $rows[1]['name']);
    }

    /**
     * Insert new contact, verify dedup query still works.
     */
    public function testDeduplicationAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_dedup_contacts VALUES (8, 'diana@example.com', 'Diana P.', '555-0008', '2024-01-08 17:00:00')");

        $rows = $this->ztdQuery(
            "SELECT email, COUNT(*) AS cnt
             FROM mi_dedup_contacts
             GROUP BY email
             HAVING COUNT(*) > 1
             ORDER BY email"
        );

        $this->assertCount(3, $rows); // alice, bob, and now diana
        $emails = array_column($rows, 'email');
        $this->assertContains('diana@example.com', $emails);
    }

    /**
     * Prepared duplicate lookup by email.
     */
    public function testPreparedDuplicateLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT id, name, created_at FROM mi_dedup_contacts WHERE email = ? ORDER BY created_at',
            ['alice@example.com']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice Smith', $rows[0]['name']);
        $this->assertSame('Alice S.', $rows[1]['name']);
        $this->assertSame('A. Smith', $rows[2]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dedup_contacts');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
