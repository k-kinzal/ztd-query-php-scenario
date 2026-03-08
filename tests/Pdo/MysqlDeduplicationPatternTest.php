<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests deduplication query patterns through ZTD shadow store (MySQL PDO).
 * @spec SPEC-10.2.38
 */
class MysqlDeduplicationPatternTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_dedup_contacts (
            id INT PRIMARY KEY,
            email VARCHAR(255),
            name VARCHAR(255),
            phone VARCHAR(50),
            created_at DATETIME
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_dedup_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_dedup_contacts VALUES (1, 'alice@example.com', 'Alice Smith', '555-0001', '2024-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_dedup_contacts VALUES (2, 'bob@example.com', 'Bob Jones', '555-0002', '2024-01-02 11:00:00')");
        $this->pdo->exec("INSERT INTO mp_dedup_contacts VALUES (3, 'alice@example.com', 'Alice S.', '555-0003', '2024-01-03 12:00:00')");
        $this->pdo->exec("INSERT INTO mp_dedup_contacts VALUES (4, 'charlie@example.com', 'Charlie Brown', '555-0004', '2024-01-04 13:00:00')");
        $this->pdo->exec("INSERT INTO mp_dedup_contacts VALUES (5, 'bob@example.com', 'Robert Jones', '555-0005', '2024-01-05 14:00:00')");
        $this->pdo->exec("INSERT INTO mp_dedup_contacts VALUES (6, 'alice@example.com', 'A. Smith', '555-0006', '2024-01-06 15:00:00')");
        $this->pdo->exec("INSERT INTO mp_dedup_contacts VALUES (7, 'diana@example.com', 'Diana Prince', '555-0007', '2024-01-07 16:00:00')");
    }

    public function testFindDuplicateEmails(): void
    {
        $rows = $this->ztdQuery(
            "SELECT email, COUNT(*) AS cnt
             FROM mp_dedup_contacts
             GROUP BY email
             HAVING COUNT(*) > 1
             ORDER BY cnt DESC, email"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('alice@example.com', $rows[0]['email']);
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * ROW_NUMBER derived table returns empty (SPEC-3.3a).
     */
    public function testRowNumberDerivedTableReturnsEmpty(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, email, name
             FROM (
                 SELECT id, email, name,
                        ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) AS rn
                 FROM mp_dedup_contacts
             ) ranked
             WHERE rn = 1
             ORDER BY email"
        );

        $this->assertCount(0, $rows, 'Derived table dedup returns empty on MySQL (SPEC-3.3a)');
    }

    public function testKeepFirstOccurrenceWorkaround(): void
    {
        $rows = $this->ztdQuery(
            "SELECT MIN(id) AS id, email, MIN(name) AS name
             FROM mp_dedup_contacts
             GROUP BY email
             ORDER BY email"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('alice@example.com', $rows[0]['email']);
    }

    public function testKeepLastOccurrenceWorkaround(): void
    {
        $rows = $this->ztdQuery(
            "SELECT MAX(id) AS id, email
             FROM mp_dedup_contacts
             GROUP BY email
             ORDER BY email"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(6, (int) $rows[0]['id']);
    }

    public function testDuplicateSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS total_rows,
                    COUNT(DISTINCT email) AS unique_emails,
                    COUNT(*) - COUNT(DISTINCT email) AS duplicate_count
             FROM mp_dedup_contacts"
        );

        $this->assertEquals(7, (int) $rows[0]['total_rows']);
        $this->assertEquals(4, (int) $rows[0]['unique_emails']);
        $this->assertEquals(3, (int) $rows[0]['duplicate_count']);
    }

    public function testFindAllDuplicateRows(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.email
             FROM mp_dedup_contacts c
             WHERE c.email IN (
                 SELECT email FROM mp_dedup_contacts
                 GROUP BY email HAVING COUNT(*) > 1
             )
             ORDER BY c.email, c.id"
        );

        $this->assertCount(5, $rows);
    }

    public function testDeleteDuplicatesKeepFirst(): void
    {
        $this->pdo->exec(
            "DELETE FROM mp_dedup_contacts WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) AS rn
                    FROM mp_dedup_contacts
                ) ranked WHERE rn > 1
            )"
        );

        $rows = $this->ztdQuery("SELECT id, email, name FROM mp_dedup_contacts ORDER BY email");
        $this->assertCount(4, $rows);
        $this->assertSame('Alice Smith', $rows[0]['name']);
    }

    public function testPreparedDuplicateLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT id, name FROM mp_dedup_contacts WHERE email = ? ORDER BY created_at',
            ['alice@example.com']
        );

        $this->assertCount(3, $rows);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_dedup_contacts');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
