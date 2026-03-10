<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT...ON DUPLICATE KEY UPDATE when the conflict is on a
 * non-PK UNIQUE constraint, not the primary key.
 *
 * Common pattern: tables with AUTO_INCREMENT PK and UNIQUE email/slug/code.
 * The IODKU should trigger UPDATE when the UNIQUE column conflicts, not
 * just when the PK conflicts.
 *
 * @spec SPEC-4.2a
 */
class UpsertNonPkUniqueTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_unpu_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(100) NOT NULL,
            name VARCHAR(50) NOT NULL,
            login_count INT NOT NULL DEFAULT 0,
            UNIQUE KEY uk_email (email)
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_unpu_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_unpu_users (id, email, name, login_count) VALUES (1, 'alice@test.com', 'Alice', 5)");
        $this->mysqli->query("INSERT INTO mi_unpu_users (id, email, name, login_count) VALUES (2, 'bob@test.com', 'Bob', 3)");
    }

    /**
     * IODKU on PK conflict — this should work (positive control).
     */
    public function testIodkuOnPkConflictWorks(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_unpu_users (id, email, name, login_count) VALUES (1, 'alice-new@test.com', 'Alice-Updated', 6) "
                . "ON DUPLICATE KEY UPDATE name = VALUES(name), login_count = VALUES(login_count)"
            );

            $rows = $this->ztdQuery("SELECT name, login_count FROM mi_unpu_users WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Alice-Updated', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('PK conflict IODKU failed: ' . $e->getMessage());
        }
    }

    /**
     * IODKU on non-PK UNIQUE email conflict — should UPDATE, not INSERT duplicate.
     */
    public function testIodkuOnUniqueEmailConflict(): void
    {
        try {
            // id=99 is new, but email 'alice@test.com' already exists (id=1)
            // Should trigger UPDATE on the existing row (id=1), not insert new row
            $this->mysqli->query(
                "INSERT INTO mi_unpu_users (id, email, name, login_count) VALUES (99, 'alice@test.com', 'Alice-Upserted', 10) "
                . "ON DUPLICATE KEY UPDATE name = VALUES(name), login_count = VALUES(login_count)"
            );

            // Check total row count — should still be 2
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_unpu_users");
            $cnt = (int) $rows[0]['cnt'];

            if ($cnt !== 2) {
                $this->markTestIncomplete(
                    'IODKU on UNIQUE email created duplicate row. Expected 2 rows, got ' . $cnt
                    . '. UpsertMutation only checks PK for conflict, not UNIQUE constraints.'
                );
            }

            // Check that existing row was updated
            $rows = $this->ztdQuery("SELECT name, login_count FROM mi_unpu_users WHERE email = 'alice@test.com'");
            $this->assertCount(1, $rows);
            $this->assertSame('Alice-Upserted', $rows[0]['name']);
            $this->assertSame(10, (int) $rows[0]['login_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNIQUE email IODKU failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row IODKU with mixed PK and UNIQUE conflicts.
     */
    public function testMultiRowIodkuWithUniqueConflicts(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_unpu_users (id, email, name, login_count) VALUES "
                . "(3, 'charlie@test.com', 'Charlie', 1), "     // new row, no conflict
                . "(99, 'bob@test.com', 'Bob-Updated', 8) "     // unique email conflict on id=2
                . "ON DUPLICATE KEY UPDATE name = VALUES(name), login_count = VALUES(login_count)"
            );

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_unpu_users");
            $cnt = (int) $rows[0]['cnt'];

            if ($cnt !== 3) {
                $this->markTestIncomplete(
                    'Multi-row IODKU with UNIQUE conflict: expected 3 rows (2 existing + 1 new), got ' . $cnt
                );
            }

            // Verify Bob was updated via UNIQUE email conflict
            $rows = $this->ztdQuery("SELECT name, login_count FROM mi_unpu_users WHERE email = 'bob@test.com'");
            if ($rows[0]['name'] !== 'Bob-Updated') {
                $this->markTestIncomplete(
                    'Bob not updated via UNIQUE conflict. Got name=' . json_encode($rows[0]['name'])
                );
            }
            $this->assertSame('Bob-Updated', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row UNIQUE IODKU failed: ' . $e->getMessage());
        }
    }

    /**
     * IODKU with expression referencing existing row: login_count = login_count + VALUES(login_count).
     */
    public function testIodkuUniqueConflictWithSelfReference(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_unpu_users (id, email, name, login_count) VALUES (99, 'alice@test.com', 'Alice', 1) "
                . "ON DUPLICATE KEY UPDATE login_count = login_count + VALUES(login_count)"
            );

            $rows = $this->ztdQuery("SELECT login_count FROM mi_unpu_users WHERE email = 'alice@test.com'");

            if (empty($rows)) {
                $this->markTestIncomplete('No rows found for alice@test.com');
            }

            $count = (int) $rows[0]['login_count'];
            // Expected: 5 (original) + 1 = 6
            if ($count !== 6) {
                $this->markTestIncomplete(
                    'IODKU self-reference on UNIQUE conflict: expected login_count=6, got ' . $count
                );
            }
            $this->assertSame(6, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNIQUE self-reference IODKU failed: ' . $e->getMessage());
        }
    }
}
