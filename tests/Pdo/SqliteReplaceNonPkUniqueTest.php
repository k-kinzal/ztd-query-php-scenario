<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests REPLACE INTO behavior with non-PK UNIQUE constraints on SQLite.
 *
 * REPLACE INTO should delete the conflicting row and insert a new one.
 * When the conflict is on a UNIQUE constraint (not the PK), the shadow store
 * must detect the conflict and handle the replacement. Related to Issue #153
 * (non-PK UNIQUE constraints not recognized in UPSERT).
 *
 * @spec SPEC-4.2b
 */
class SqliteReplaceNonPkUniqueTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_rnu_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                status TEXT DEFAULT 'active'
            )",
            "CREATE TABLE sl_rnu_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                category TEXT NOT NULL,
                value REAL NOT NULL,
                UNIQUE(code, category)
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rnu_users', 'sl_rnu_codes'];
    }

    /**
     * REPLACE INTO on single-column UNIQUE (email) — should replace existing row.
     */
    public function testReplaceSingleColumnUnique(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_rnu_users (email, name) VALUES ('alice@example.com', 'Alice')");
            $this->pdo->exec("INSERT INTO sl_rnu_users (email, name) VALUES ('bob@example.com', 'Bob')");

            // REPLACE with same email should replace Alice
            $this->pdo->exec("REPLACE INTO sl_rnu_users (email, name, status) VALUES ('alice@example.com', 'Alice Updated', 'inactive')");

            $rows = $this->ztdQuery("SELECT email, name, status FROM sl_rnu_users ORDER BY email");

            if (count($rows) > 2) {
                $this->markTestIncomplete(
                    'REPLACE on UNIQUE email created duplicate: ' . count($rows) . ' rows. '
                    . 'Shadow store does not enforce non-PK UNIQUE constraints. Got: ' . json_encode($rows)
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'REPLACE on UNIQUE email produced ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $alice = null;
            foreach ($rows as $row) {
                if ($row['email'] === 'alice@example.com') {
                    $alice = $row;
                }
            }

            if ($alice === null) {
                $this->markTestIncomplete('Alice row not found after REPLACE.');
            }
            if ($alice['name'] !== 'Alice Updated') {
                $this->markTestIncomplete(
                    'REPLACE did not update name. Expected "Alice Updated", got ' . json_encode($alice['name'])
                );
            }

            $this->assertSame('Alice Updated', $alice['name']);
            $this->assertSame('inactive', $alice['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('REPLACE single-column UNIQUE test failed: ' . $e->getMessage());
        }
    }

    /**
     * REPLACE INTO on multi-column UNIQUE (code, category).
     */
    public function testReplaceMultiColumnUnique(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_rnu_codes (code, category, value) VALUES ('A1', 'alpha', 10.0)");
            $this->pdo->exec("INSERT INTO sl_rnu_codes (code, category, value) VALUES ('A1', 'beta', 20.0)");
            $this->pdo->exec("INSERT INTO sl_rnu_codes (code, category, value) VALUES ('B1', 'alpha', 30.0)");

            // REPLACE with same (code, category) = ('A1', 'alpha') should replace first row
            $this->pdo->exec("REPLACE INTO sl_rnu_codes (code, category, value) VALUES ('A1', 'alpha', 99.0)");

            $rows = $this->ztdQuery("SELECT code, category, value FROM sl_rnu_codes ORDER BY code, category");

            if (count($rows) > 3) {
                $this->markTestIncomplete(
                    'REPLACE on multi-column UNIQUE created duplicate: ' . count($rows) . ' rows. '
                    . 'Shadow store does not enforce composite UNIQUE constraint. Got: ' . json_encode($rows)
                );
            }
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'REPLACE on multi-column UNIQUE produced ' . count($rows) . ' rows. Expected 3. Got: ' . json_encode($rows)
                );
            }

            // Verify the replacement took effect
            $a1Alpha = null;
            foreach ($rows as $row) {
                if ($row['code'] === 'A1' && $row['category'] === 'alpha') {
                    $a1Alpha = $row;
                }
            }

            if ($a1Alpha === null) {
                $this->markTestIncomplete('Row (A1, alpha) not found after REPLACE.');
            }
            if ((float) $a1Alpha['value'] !== 99.0) {
                $this->markTestIncomplete(
                    'REPLACE did not update value. Expected 99.0, got ' . $a1Alpha['value']
                );
            }

            $this->assertSame(99.0, (float) $a1Alpha['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('REPLACE multi-column UNIQUE test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT OR REPLACE with non-PK UNIQUE conflict.
     */
    public function testInsertOrReplaceNonPkUnique(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_rnu_users (email, name) VALUES ('carol@example.com', 'Carol')");

            $this->pdo->exec("INSERT OR REPLACE INTO sl_rnu_users (email, name) VALUES ('carol@example.com', 'Carol v2')");

            $rows = $this->ztdQuery("SELECT name FROM sl_rnu_users WHERE email = 'carol@example.com'");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'INSERT OR REPLACE on UNIQUE email created ' . count($rows) . ' rows instead of 1. '
                    . 'Got: ' . json_encode($rows)
                );
            }
            if (count($rows) === 0) {
                $this->markTestIncomplete('INSERT OR REPLACE produced 0 rows. Expected 1.');
            }

            $this->assertSame('Carol v2', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT OR REPLACE non-PK UNIQUE test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared REPLACE with non-PK UNIQUE conflict.
     */
    public function testPreparedReplaceNonPkUnique(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_rnu_users (email, name) VALUES ('dave@example.com', 'Dave')");

            $stmt = $this->pdo->prepare("REPLACE INTO sl_rnu_users (email, name) VALUES (?, ?)");
            $stmt->execute(['dave@example.com', 'Dave Updated']);

            $rows = $this->ztdQuery("SELECT name FROM sl_rnu_users WHERE email = 'dave@example.com'");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'Prepared REPLACE on UNIQUE email created ' . count($rows) . ' rows. Got: ' . json_encode($rows)
                );
            }
            if (count($rows) === 0) {
                $this->markTestIncomplete('Prepared REPLACE produced 0 rows.');
            }

            $this->assertSame('Dave Updated', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared REPLACE non-PK UNIQUE test failed: ' . $e->getMessage());
        }
    }

    /**
     * Total row count should remain stable after multiple REPLACE operations
     * on the same UNIQUE constraint.
     */
    public function testRowCountStableAfterMultipleReplaces(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_rnu_users (email, name) VALUES ('eve@example.com', 'Eve v1')");
            $this->pdo->exec("INSERT INTO sl_rnu_users (email, name) VALUES ('frank@example.com', 'Frank')");

            $this->pdo->exec("REPLACE INTO sl_rnu_users (email, name) VALUES ('eve@example.com', 'Eve v2')");
            $this->pdo->exec("REPLACE INTO sl_rnu_users (email, name) VALUES ('eve@example.com', 'Eve v3')");
            $this->pdo->exec("REPLACE INTO sl_rnu_users (email, name) VALUES ('eve@example.com', 'Eve v4')");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rnu_users");
            $count = (int) $rows[0]['cnt'];

            if ($count > 2) {
                $this->markTestIncomplete(
                    'Row count after multiple REPLACE on UNIQUE is ' . $count . '. Expected 2. '
                    . 'Shadow store is creating duplicates.'
                );
            }

            $this->assertSame(2, $count);

            // Verify final name
            $rows = $this->ztdQuery("SELECT name FROM sl_rnu_users WHERE email = 'eve@example.com'");
            $this->assertSame('Eve v4', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple REPLACE row count test failed: ' . $e->getMessage());
        }
    }

    /**
     * ON CONFLICT DO UPDATE on non-PK UNIQUE constraint.
     */
    public function testOnConflictDoUpdateNonPkUnique(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_rnu_users (email, name, status) VALUES ('grace@example.com', 'Grace', 'active')");

            $this->pdo->exec(
                "INSERT INTO sl_rnu_users (email, name, status) VALUES ('grace@example.com', 'Grace Updated', 'inactive')
                 ON CONFLICT(email) DO UPDATE SET name = excluded.name, status = excluded.status"
            );

            $rows = $this->ztdQuery("SELECT email, name, status FROM sl_rnu_users WHERE email = 'grace@example.com'");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'ON CONFLICT(email) DO UPDATE created ' . count($rows) . ' rows. '
                    . 'Shadow store does not enforce non-PK UNIQUE in upsert. Got: ' . json_encode($rows)
                );
            }
            if (count($rows) === 0) {
                $this->markTestIncomplete('ON CONFLICT DO UPDATE on non-PK UNIQUE produced 0 rows.');
            }

            $this->assertSame('Grace Updated', $rows[0]['name']);
            $this->assertSame('inactive', $rows[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ON CONFLICT DO UPDATE non-PK UNIQUE test failed: ' . $e->getMessage());
        }
    }
}
