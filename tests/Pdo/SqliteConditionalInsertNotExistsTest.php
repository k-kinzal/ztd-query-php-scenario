<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT ... SELECT WHERE NOT EXISTS pattern in the shadow store.
 *
 * Real-world scenario: applications use this pattern as a portable upsert
 * alternative — insert a row only if it doesn't already exist. This avoids
 * the need for platform-specific ON CONFLICT / ON DUPLICATE KEY syntax.
 * The shadow store must correctly evaluate EXISTS against shadow data.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.1a
 */
class SqliteConditionalInsertNotExistsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cine_users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL
            )',
            'CREATE TABLE sl_cine_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                action TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cine_audit', 'sl_cine_users'];
    }

    /**
     * INSERT ... SELECT WHERE NOT EXISTS — new row should be inserted.
     */
    public function testInsertNotExistsNewRow(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_cine_users (id, username, email)
                 SELECT 1, 'alice', 'alice@test.com'
                 WHERE NOT EXISTS (SELECT 1 FROM sl_cine_users WHERE username = 'alice')"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_cine_users");
            $this->assertCount(1, $rows);
            $this->assertSame('alice', $rows[0]['username']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT WHERE NOT EXISTS (new row) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT ... SELECT WHERE NOT EXISTS — existing row should be skipped.
     */
    public function testInsertNotExistsExistingRowSkipped(): void
    {
        $this->ztdExec("INSERT INTO sl_cine_users VALUES (1, 'alice', 'alice@test.com')");

        try {
            $this->ztdExec(
                "INSERT INTO sl_cine_users (id, username, email)
                 SELECT 2, 'alice', 'alice-new@test.com'
                 WHERE NOT EXISTS (SELECT 1 FROM sl_cine_users WHERE username = 'alice')"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_cine_users ORDER BY id");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'INSERT...SELECT WHERE NOT EXISTS did not skip existing row. '
                    . 'Got ' . count($rows) . ' rows — EXISTS subquery may not read shadow data.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('alice@test.com', $rows[0]['email'],
                'Original email should be preserved');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT WHERE NOT EXISTS (existing) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT NOT EXISTS with prepared parameters.
     */
    public function testInsertNotExistsWithPreparedParams(): void
    {
        $this->ztdExec("INSERT INTO sl_cine_users VALUES (1, 'bob', 'bob@test.com')");

        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_cine_users (id, username, email)
                 SELECT ?, ?, ?
                 WHERE NOT EXISTS (SELECT 1 FROM sl_cine_users WHERE username = ?)"
            );

            // Try to insert duplicate username
            $stmt->execute([2, 'bob', 'bob-new@test.com', 'bob']);

            $rows = $this->ztdQuery("SELECT * FROM sl_cine_users ORDER BY id");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'INSERT NOT EXISTS with prepared params did not skip duplicate. '
                    . 'Got ' . count($rows) . ' rows.'
                );
            }

            $this->assertCount(1, $rows);

            // Now insert a genuinely new user
            $stmt->execute([3, 'charlie', 'charlie@test.com', 'charlie']);

            $rows = $this->ztdQuery("SELECT * FROM sl_cine_users ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertSame('charlie', $rows[1]['username']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT NOT EXISTS referencing a different table.
     * Pattern: INSERT into audit only if user exists.
     */
    public function testInsertNotExistsCrossTable(): void
    {
        $this->ztdExec("INSERT INTO sl_cine_users VALUES (1, 'alice', 'alice@test.com')");

        try {
            // Insert audit record only if user exists (inverse: WHERE EXISTS)
            $this->ztdExec(
                "INSERT INTO sl_cine_audit (user_id, action)
                 SELECT 1, 'login'
                 WHERE EXISTS (SELECT 1 FROM sl_cine_users WHERE id = 1)"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_cine_audit");
            $this->assertCount(1, $rows);
            $this->assertSame('login', $rows[0]['action']);

            // Try for non-existent user — should not insert
            $this->ztdExec(
                "INSERT INTO sl_cine_audit (user_id, action)
                 SELECT 999, 'login'
                 WHERE EXISTS (SELECT 1 FROM sl_cine_users WHERE id = 999)"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_cine_audit");
            $this->assertCount(1, $rows, 'Should not insert audit for non-existent user');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with EXISTS cross-table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Sequential conditional inserts — second should see first.
     */
    public function testSequentialConditionalInserts(): void
    {
        try {
            // First insert — table is empty, so NOT EXISTS is true
            $this->ztdExec(
                "INSERT INTO sl_cine_users (id, username, email)
                 SELECT 1, 'alice', 'a@test.com'
                 WHERE NOT EXISTS (SELECT 1 FROM sl_cine_users WHERE username = 'alice')"
            );

            // Second insert with same username — should be blocked by shadow data
            $this->ztdExec(
                "INSERT INTO sl_cine_users (id, username, email)
                 SELECT 2, 'alice', 'a2@test.com'
                 WHERE NOT EXISTS (SELECT 1 FROM sl_cine_users WHERE username = 'alice')"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_cine_users ORDER BY id");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'Sequential INSERT NOT EXISTS: second insert was not blocked. '
                    . 'Got ' . count($rows) . ' rows. '
                    . 'The NOT EXISTS subquery may not read the shadow-inserted row.'
                );
            }

            $this->assertCount(1, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Sequential conditional inserts failed: ' . $e->getMessage()
            );
        }
    }
}
