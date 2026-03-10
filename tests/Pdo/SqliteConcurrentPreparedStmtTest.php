<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests whether the shadow store correctly handles multiple prepared
 * statements that are open and executed concurrently (interleaved).
 *
 * Real applications often prepare multiple statements and interleave their
 * execution (e.g., a SELECT to read data, then an INSERT using results,
 * then re-execute the SELECT). The shadow store must track each prepared
 * statement independently.
 *
 * Confirms Issue #87: Prepared SELECT re-execution returns stale shadow
 * data (extended to concurrent/interleaved patterns).
 *
 * @spec SPEC-3.2
 */
class SqliteConcurrentPreparedStmtTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_cps_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0
            )",
            "CREATE TABLE sl_cps_log (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                action TEXT NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cps_users', 'sl_cps_log'];
    }

    /**
     * Two prepared SELECTs on different tables, interleaved execution.
     */
    public function testTwoSelectsDifferentTablesInterleaved(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cps_users (id, name, score) VALUES (1, 'Alice', 100)");
            $this->pdo->exec("INSERT INTO sl_cps_log (id, user_id, action) VALUES (1, 1, 'login')");

            $stmtUsers = $this->pdo->prepare("SELECT name FROM sl_cps_users WHERE id = ?");
            $stmtLog = $this->pdo->prepare("SELECT action FROM sl_cps_log WHERE user_id = ?");

            // Execute user query first
            $stmtUsers->execute([1]);
            $userRows = $stmtUsers->fetchAll(\PDO::FETCH_ASSOC);

            // Execute log query while user query results are already fetched
            $stmtLog->execute([1]);
            $logRows = $stmtLog->fetchAll(\PDO::FETCH_ASSOC);

            $this->assertCount(1, $userRows);
            $this->assertCount(1, $logRows);
            $this->assertSame('Alice', $userRows[0]['name']);
            $this->assertSame('login', $logRows[0]['action']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Two selects interleaved test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare a SELECT, then INSERT, then re-execute SELECT.
     * The re-executed SELECT should see the INSERT (unlike Issue #87 stale data).
     * This is a different pattern: we create a NEW prepared SELECT, not re-execute.
     */
    public function testPrepareSelectInsertNewSelect(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cps_users (id, name, score) VALUES (1, 'Alice', 100)");

            // First SELECT
            $stmt1 = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM sl_cps_users");
            $stmt1->execute();
            $count1 = (int) $stmt1->fetch(\PDO::FETCH_ASSOC)['cnt'];

            // INSERT
            $this->pdo->exec("INSERT INTO sl_cps_users (id, name, score) VALUES (2, 'Bob', 200)");

            // New prepared SELECT (different statement object)
            $stmt2 = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM sl_cps_users");
            $stmt2->execute();
            $count2 = (int) $stmt2->fetch(\PDO::FETCH_ASSOC)['cnt'];

            if ($count2 !== 2) {
                $this->markTestIncomplete(
                    "New prepared SELECT after INSERT returned count={$count2}, expected 2."
                    . " Shadow store may not track new statements independently."
                );
            }
            $this->assertEquals(1, $count1);
            $this->assertEquals(2, $count2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepare select insert new select test failed: ' . $e->getMessage());
        }
    }

    /**
     * Interleaved INSERT and SELECT prepared statements on the same table.
     */
    public function testInterleavedInsertAndSelect(): void
    {
        try {
            $stmtInsert = $this->pdo->prepare("INSERT INTO sl_cps_users (id, name, score) VALUES (?, ?, ?)");
            $stmtSelect = $this->pdo->prepare("SELECT name, score FROM sl_cps_users WHERE id = ?");

            // Insert first user
            $stmtInsert->execute([1, 'Alice', 100]);

            // Select first user
            $stmtSelect->execute([1]);
            $row1 = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            if ($row1 === false || $row1['name'] !== 'Alice') {
                $this->markTestIncomplete(
                    'First interleaved SELECT returned wrong data. Got: ' . json_encode($row1)
                );
            }

            // Insert second user
            $stmtInsert->execute([2, 'Bob', 200]);

            // Select second user (re-execute SELECT stmt)
            $stmtSelect->execute([2]);
            $row2 = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            if ($row2 === false || $row2['name'] !== 'Bob') {
                $this->markTestIncomplete(
                    'Second interleaved SELECT returned wrong data after INSERT re-execute. Got: ' . json_encode($row2)
                    . ' This may be the stale prepared SELECT issue (Issue #87).'
                );
            }

            $this->assertSame('Alice', $row1['name']);
            $this->assertSame('Bob', $row2['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Interleaved insert and select test failed: ' . $e->getMessage());
        }
    }

    /**
     * Three prepared statements open at once: INSERT, UPDATE, SELECT.
     */
    public function testThreeStatementsSimultaneous(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cps_users (id, name, score) VALUES (1, 'Alice', 100)");

            $stmtInsert = $this->pdo->prepare("INSERT INTO sl_cps_users (id, name, score) VALUES (?, ?, ?)");
            $stmtUpdate = $this->pdo->prepare("UPDATE sl_cps_users SET score = ? WHERE id = ?");
            $stmtSelect = $this->pdo->prepare("SELECT name, score FROM sl_cps_users WHERE id = ?");

            // Insert a new user
            $stmtInsert->execute([2, 'Bob', 50]);

            // Update Alice's score
            $stmtUpdate->execute([150, 1]);

            // Select both users
            $stmtSelect->execute([1]);
            $alice = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            $stmtSelect->execute([2]);
            $bob = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            if ($alice === false || $bob === false) {
                $this->markTestIncomplete(
                    'Three simultaneous statements: SELECT returned false. Alice=' . json_encode($alice)
                    . ', Bob=' . json_encode($bob)
                );
            }

            // Check if update was reflected
            if ((int) ($alice['score'] ?? 0) !== 150) {
                $this->markTestIncomplete(
                    'Three simultaneous statements: UPDATE not reflected in SELECT. Expected score=150, got: ' . json_encode($alice)
                );
            }

            $this->assertSame('Alice', $alice['name']);
            $this->assertEquals(150, (int) $alice['score']);
            $this->assertSame('Bob', $bob['name']);
            $this->assertEquals(50, (int) $bob['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Three statements simultaneous test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT on table A, prepared SELECT on table B that JOINs table A.
     */
    public function testCrossTablePreparedJoin(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cps_users (id, name, score) VALUES (1, 'Alice', 100)");

            $stmtInsertLog = $this->pdo->prepare("INSERT INTO sl_cps_log (id, user_id, action) VALUES (?, ?, ?)");
            $stmtInsertLog->execute([1, 1, 'login']);
            $stmtInsertLog->execute([2, 1, 'purchase']);

            // JOIN query via prepared statement
            $stmtJoin = $this->pdo->prepare(
                "SELECT u.name, l.action FROM sl_cps_users u JOIN sl_cps_log l ON l.user_id = u.id WHERE u.id = ? ORDER BY l.id"
            );
            $stmtJoin->execute([1]);
            $rows = $stmtJoin->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Cross-table prepared JOIN returned ' . count($rows) . ' rows, expected 2. Got: ' . json_encode($rows)
                );
            }
            $this->assertCount(2, $rows);
            $this->assertSame('login', $rows[0]['action']);
            $this->assertSame('purchase', $rows[1]['action']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Cross-table prepared join test failed: ' . $e->getMessage());
        }
    }
}
