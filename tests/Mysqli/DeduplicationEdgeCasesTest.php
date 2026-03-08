<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Extended deduplication: composite-key dedup, NULL in uniqueness column,
 * multi-pass dedup, and dedup with aggregate tiebreaker.
 * @spec SPEC-10.2.38
 */
class DeduplicationEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_dde_contacts (
            id INT PRIMARY KEY,
            email VARCHAR(255),
            name VARCHAR(255),
            source VARCHAR(50),
            score INT,
            created_at VARCHAR(20)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_dde_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (1, 'alice@test.com', 'Alice A', 'web', 80, '2024-01-01')");
        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (2, 'alice@test.com', 'Alice B', 'import', 90, '2024-02-01')");
        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (3, 'bob@test.com', 'Bob', 'web', 70, '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (4, 'bob@test.com', 'Bobby', 'api', 60, '2024-03-01')");
        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (5, NULL, 'Unknown1', 'import', 50, '2024-01-10')");
        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (6, NULL, 'Unknown2', 'import', 40, '2024-02-10')");
        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (7, 'charlie@test.com', 'Charlie', 'web', 95, '2024-01-20')");
    }

    public function testFindDuplicatesByEmail(): void
    {
        $rows = $this->ztdQuery("
            SELECT email, COUNT(*) AS cnt
            FROM mi_dde_contacts
            WHERE email IS NOT NULL
            GROUP BY email
            HAVING COUNT(*) > 1
            ORDER BY email
        ");
        $this->assertCount(2, $rows); // alice, bob
        $this->assertSame('alice@test.com', $rows[0]['email']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testKeepHighestScorePerEmail(): void
    {
        // For each email, find the row with the highest score (GROUP BY + MAX workaround)
        $rows = $this->ztdQuery("
            SELECT c.id, c.email, c.name, c.score
            FROM mi_dde_contacts c
            JOIN (
                SELECT email, MAX(score) AS max_score
                FROM mi_dde_contacts
                WHERE email IS NOT NULL
                GROUP BY email
            ) best ON c.email = best.email AND c.score = best.max_score
            ORDER BY c.email
        ");
        $this->assertCount(3, $rows);
        $this->assertSame('alice@test.com', $rows[0]['email']);
        $this->assertSame(90, (int) $rows[0]['score']); // Alice B has higher score
        $this->assertSame('bob@test.com', $rows[1]['email']);
        $this->assertSame(70, (int) $rows[1]['score']); // Bob (id=3) has higher score
    }

    public function testKeepFirstCreatedPerEmail(): void
    {
        // Keep earliest created entry per email using MIN(id) as proxy for first
        $rows = $this->ztdQuery("
            SELECT c.id, c.email, c.name
            FROM mi_dde_contacts c
            JOIN (
                SELECT email, MIN(id) AS first_id
                FROM mi_dde_contacts
                WHERE email IS NOT NULL
                GROUP BY email
            ) first_entry ON c.id = first_entry.first_id
            ORDER BY c.email
        ");
        $this->assertCount(3, $rows);
        $this->assertSame(1, (int) $rows[0]['id']); // Alice A (first)
        $this->assertSame(3, (int) $rows[1]['id']); // Bob (first)
        $this->assertSame(7, (int) $rows[2]['id']); // Charlie (unique)
    }

    public function testNullsNotGroupedAsDuplicates(): void
    {
        // NULL emails should not be grouped together
        $rows = $this->ztdQuery("
            SELECT email, COUNT(*) AS cnt
            FROM mi_dde_contacts
            GROUP BY email
            HAVING COUNT(*) > 1
            ORDER BY email
        ");
        // With standard SQL, GROUP BY treats NULLs as equal
        // So NULL group has count=2 which matches HAVING > 1
        $nullGroups = array_filter($rows, fn($r) => $r['email'] === null);
        $this->assertCount(1, $nullGroups);
    }

    public function testCompositeKeyDedup(): void
    {
        // Dedup by email + source combination
        $rows = $this->ztdQuery("
            SELECT email, source, COUNT(*) AS cnt
            FROM mi_dde_contacts
            WHERE email IS NOT NULL
            GROUP BY email, source
            HAVING COUNT(*) > 1
        ");
        // No duplicates when considering email+source together (each is unique)
        $this->assertCount(0, $rows);
    }

    public function testDeleteDuplicatesKeepFirst(): void
    {
        // Delete all but the first (MIN id) per email
        $this->mysqli->query("
            DELETE FROM mi_dde_contacts
            WHERE email IS NOT NULL
            AND id NOT IN (
                SELECT min_id FROM (
                    SELECT MIN(id) AS min_id
                    FROM mi_dde_contacts
                    WHERE email IS NOT NULL
                    GROUP BY email
                ) AS keep_ids
            )
        ");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dde_contacts WHERE email IS NOT NULL");
        $this->assertSame(3, (int) $rows[0]['cnt']); // alice(1), bob(3), charlie(7)

        // Verify correct ones kept
        $rows = $this->ztdQuery("SELECT id FROM mi_dde_contacts WHERE email = 'alice@test.com'");
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testDedupSummaryStats(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT email) AS unique_emails,
                COUNT(*) - COUNT(DISTINCT email) AS duplicate_count
            FROM mi_dde_contacts
            WHERE email IS NOT NULL
        ");
        $this->assertSame(5, (int) $rows[0]['total_rows']);
        $this->assertSame(3, (int) $rows[0]['unique_emails']);
        $this->assertSame(2, (int) $rows[0]['duplicate_count']);
    }

    public function testMultiPassDedup(): void
    {
        // First pass: delete low-score duplicates
        $this->mysqli->query("
            DELETE FROM mi_dde_contacts
            WHERE email IS NOT NULL
            AND id NOT IN (
                SELECT min_id FROM (
                    SELECT MIN(id) AS min_id FROM mi_dde_contacts WHERE email IS NOT NULL
                    GROUP BY email
                ) AS keep_ids
            )
            AND score < 80
        ");

        // Bob's duplicate (id=4, score=60) should be deleted
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dde_contacts WHERE email = 'bob@test.com'");
        $this->assertSame(1, (int) $rows[0]['cnt']);

        // Alice's duplicate (id=2, score=90) should NOT be deleted (score >= 80)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dde_contacts WHERE email = 'alice@test.com'");
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testPreparedDuplicateLookupByEmail(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, score FROM mi_dde_contacts WHERE email = ? ORDER BY score DESC",
            ['alice@test.com']
        );
        $this->assertCount(2, $rows);
        $this->assertSame(90, (int) $rows[0]['score']); // highest first
    }

    public function testDedupAfterInsert(): void
    {
        // Insert a new duplicate
        $this->mysqli->query("INSERT INTO mi_dde_contacts VALUES (8, 'charlie@test.com', 'Charles', 'api', 85, '2024-04-01')");

        $rows = $this->ztdQuery("
            SELECT email, COUNT(*) AS cnt
            FROM mi_dde_contacts
            WHERE email IS NOT NULL
            GROUP BY email
            HAVING COUNT(*) > 1
            ORDER BY email
        ");
        $this->assertCount(3, $rows); // alice, bob, charlie now all have dupes
    }
}
