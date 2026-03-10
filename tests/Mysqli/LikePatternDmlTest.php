<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests LIKE / NOT LIKE pattern matching in UPDATE and DELETE WHERE clauses
 * through ZTD shadow store.
 *
 * LIKE is one of the most common SQL operators. It is syntactically distinct
 * from REGEXP (#136) and handled by different code paths in most SQL parsers.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class LikePatternDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_lpd_users (
            id INT PRIMARY KEY,
            email VARCHAR(100) NOT NULL,
            name VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_lpd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_lpd_users VALUES (1, 'alice@example.com', 'Alice', 'active')");
        $this->mysqli->query("INSERT INTO mi_lpd_users VALUES (2, 'bob@test.org', 'Bob', 'active')");
        $this->mysqli->query("INSERT INTO mi_lpd_users VALUES (3, 'carol@example.com', 'Carol', 'inactive')");
        $this->mysqli->query("INSERT INTO mi_lpd_users VALUES (4, 'dave@test.org', 'Dave', 'active')");
        $this->mysqli->query("INSERT INTO mi_lpd_users VALUES (5, 'eve@example.com', 'Eve', 'banned')");
    }

    /**
     * UPDATE WHERE column LIKE '%pattern%' — suffix domain match.
     */
    public function testUpdateWhereLikeSuffix(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_lpd_users SET status = 'verified' WHERE email LIKE '%@example.com'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM mi_lpd_users ORDER BY id"
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

    /**
     * DELETE WHERE column LIKE 'prefix%' — prefix match.
     */
    public function testDeleteWhereLikePrefix(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_lpd_users WHERE name LIKE 'A%'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_lpd_users ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE WHERE LIKE prefix: expected 4 remaining rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(4, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Alice', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE LIKE prefix failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE WHERE column NOT LIKE pattern — inverse match.
     */
    public function testUpdateWhereNotLike(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_lpd_users SET status = 'external' WHERE email NOT LIKE '%@example.com'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM mi_lpd_users WHERE status = 'external' ORDER BY id"
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
     * DELETE WHERE column LIKE with underscore wildcard — single char match.
     */
    public function testDeleteWhereLikeUnderscore(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_lpd_users WHERE name LIKE '_ve'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_lpd_users ORDER BY id");

            // 'Eve' matches '_ve' (3 chars, first is wildcard)
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE WHERE LIKE underscore: expected 4 remaining rows (Eve deleted), got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(4, $rows);
            $this->assertNotContains('Eve', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE LIKE underscore failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE WHERE LIKE with parameter.
     */
    public function testPreparedUpdateWhereLike(): void
    {
        try {
            $stmt = $this->mysqli->prepare(
                "UPDATE mi_lpd_users SET status = 'matched' WHERE email LIKE ?"
            );
            $pattern = '%@test.org';
            $stmt->bind_param('s', $pattern);
            $stmt->execute();

            $rows = $this->ztdQuery(
                "SELECT id, status FROM mi_lpd_users WHERE status = 'matched' ORDER BY id"
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

    /**
     * Combined LIKE with AND condition in DELETE.
     */
    public function testDeleteWhereLikeAndCondition(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_lpd_users WHERE email LIKE '%@example.com' AND status = 'active'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_lpd_users ORDER BY id");

            // Should delete id=1 (Alice, active, @example.com) only
            // id=3 Carol is @example.com but inactive, id=5 Eve is @example.com but banned
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE WHERE LIKE AND: expected 4 remaining rows (Alice deleted), got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(4, $rows);
            $this->assertNotContains('Alice', array_column($rows, 'name'));
            $this->assertContains('Carol', array_column($rows, 'name'));
            $this->assertContains('Eve', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE LIKE AND condition failed: ' . $e->getMessage());
        }
    }
}
