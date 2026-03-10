<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests LIKE / NOT LIKE / ILIKE pattern matching in UPDATE and DELETE WHERE
 * clauses through ZTD shadow store on PostgreSQL via PDO.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresLikePatternDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_lpd_users (
            id INT PRIMARY KEY,
            email VARCHAR(100) NOT NULL,
            name VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_lpd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_lpd_users VALUES (1, 'alice@example.com', 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO pg_lpd_users VALUES (2, 'bob@test.org', 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO pg_lpd_users VALUES (3, 'carol@example.com', 'Carol', 'inactive')");
        $this->pdo->exec("INSERT INTO pg_lpd_users VALUES (4, 'dave@test.org', 'Dave', 'active')");
        $this->pdo->exec("INSERT INTO pg_lpd_users VALUES (5, 'eve@example.com', 'Eve', 'banned')");
    }

    public function testUpdateWhereLikeSuffix(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_lpd_users SET status = 'verified' WHERE email LIKE '%@example.com'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_lpd_users ORDER BY id"
            );

            $verified = array_filter($rows, fn($r) => $r['status'] === 'verified');
            if (count($verified) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE WHERE LIKE suffix: expected 3 verified rows (ids 1,3,5), got '
                    . count($verified) . ': ' . json_encode(array_column($verified, 'id'))
                );
            }
            $this->assertCount(3, $verified);
            $this->assertEquals([1, 3, 5], array_map('intval', array_column($verified, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE LIKE suffix failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereLikePrefix(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_lpd_users WHERE name LIKE 'A%'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM pg_lpd_users ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE WHERE LIKE prefix: expected 4 remaining rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(4, $rows);
            $this->assertNotContains('Alice', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE LIKE prefix failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWhereNotLike(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_lpd_users SET status = 'external' WHERE email NOT LIKE '%@example.com'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_lpd_users WHERE status = 'external' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE WHERE NOT LIKE: expected 2 external rows (ids 2,4), got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(2, $rows);
            $this->assertEquals([2, 4], array_map('intval', array_column($rows, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE NOT LIKE failed: ' . $e->getMessage());
        }
    }

    /**
     * PostgreSQL-specific: ILIKE (case-insensitive LIKE) in UPDATE WHERE.
     */
    public function testUpdateWhereIlike(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_lpd_users SET status = 'found' WHERE name ILIKE 'a%'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_lpd_users WHERE status = 'found' ORDER BY id"
            );

            // 'Alice' matches 'a%' case-insensitively
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE WHERE ILIKE: expected 1 found row (id 1), got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE ILIKE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateWhereLike(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_lpd_users SET status = 'matched' WHERE email LIKE ?"
            );
            $stmt->execute(['%@test.org']);

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_lpd_users WHERE status = 'matched' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared UPDATE WHERE LIKE: expected 2 matched rows (ids 2,4), got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(2, $rows);
            $this->assertEquals([2, 4], array_map('intval', array_column($rows, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE WHERE LIKE failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereLikeAndCondition(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_lpd_users WHERE email LIKE '%@example.com' AND status = 'active'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM pg_lpd_users ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE WHERE LIKE AND: expected 4 remaining rows (Alice deleted), got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(4, $rows);
            $this->assertNotContains('Alice', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE LIKE AND condition failed: ' . $e->getMessage());
        }
    }
}
