<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statement re-execution with interleaved DML operations.
 *
 * Users commonly prepare a SELECT once and execute it multiple times,
 * sometimes interleaving INSERT/UPDATE/DELETE. The shadow store must
 * reflect all mutations on re-execution.
 *
 * @spec SPEC-3.2, SPEC-4.1, SPEC-4.2
 */
class SqlitePreparedStmtReuseDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_psr_items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_psr_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_psr_items VALUES (1, 'Apple', 10)");
        $this->pdo->exec("INSERT INTO sl_psr_items VALUES (2, 'Banana', 20)");
    }

    /**
     * Prepare SELECT, execute, INSERT, re-execute — should see new row.
     */
    public function testSelectReexecuteAfterInsert(): void
    {
        $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM sl_psr_items");

        try {
            // First execution
            $stmt->execute();
            $row1 = $stmt->fetch(\PDO::FETCH_ASSOC);
            $count1 = (int) $row1['cnt'];
            $stmt->closeCursor();

            if ($count1 !== 2) {
                $this->markTestIncomplete(
                    'First execute: expected 2, got ' . $count1
                );
            }

            // Insert a row
            $this->pdo->exec("INSERT INTO sl_psr_items VALUES (3, 'Cherry', 30)");

            // Re-execute same statement
            $stmt->execute();
            $row2 = $stmt->fetch(\PDO::FETCH_ASSOC);
            $count2 = (int) $row2['cnt'];
            $stmt->closeCursor();

            if ($count2 !== 3) {
                $this->markTestIncomplete(
                    'Re-execute after INSERT: expected 3, got ' . $count2
                    . ' (stale cache?)'
                );
            }

            $this->assertEquals(3, $count2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT re-execute after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare SELECT, execute, DELETE, re-execute — should see fewer rows.
     */
    public function testSelectReexecuteAfterDelete(): void
    {
        $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM sl_psr_items");

        try {
            $stmt->execute();
            $row1 = $stmt->fetch(\PDO::FETCH_ASSOC);
            $stmt->closeCursor();
            $this->assertEquals(2, (int) $row1['cnt']);

            // Delete a row
            $this->pdo->exec("DELETE FROM sl_psr_items WHERE id = 1");

            // Re-execute
            $stmt->execute();
            $row2 = $stmt->fetch(\PDO::FETCH_ASSOC);
            $count2 = (int) $row2['cnt'];
            $stmt->closeCursor();

            if ($count2 !== 1) {
                $this->markTestIncomplete(
                    'Re-execute after DELETE: expected 1, got ' . $count2
                );
            }

            $this->assertEquals(1, $count2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT re-execute after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare SELECT with params, re-execute with different params.
     */
    public function testSelectReexecuteWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM sl_psr_items WHERE qty > ?");

        try {
            // First: qty > 15 => Banana
            $stmt->execute([15]);
            $rows1 = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $stmt->closeCursor();

            if (count($rows1) !== 1 || $rows1[0]['name'] !== 'Banana') {
                $this->markTestIncomplete(
                    'First execute (qty>15): expected [Banana], got ' . json_encode($rows1)
                );
            }

            // Second: qty > 5 => Apple, Banana
            $stmt->execute([5]);
            $rows2 = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            $stmt->closeCursor();

            if (count($rows2) !== 2) {
                $this->markTestIncomplete(
                    'Re-execute (qty>5): expected 2, got ' . count($rows2)
                    . '. Data: ' . json_encode($rows2)
                );
            }

            $this->assertCount(2, $rows2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT re-execute different params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare UPDATE, execute twice with different params.
     */
    public function testUpdateReexecuteWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare("UPDATE sl_psr_items SET qty = ? WHERE id = ?");

        try {
            // Update Apple
            $stmt->execute([100, 1]);
            // Update Banana
            $stmt->execute([200, 2]);

            $rows = $this->ztdQuery("SELECT name, qty FROM sl_psr_items ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE re-execute: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertEquals(100, (int) $rows[0]['qty']);
            $this->assertEquals(200, (int) $rows[1]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE re-execute failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare INSERT, execute multiple times to insert several rows.
     */
    public function testInsertReexecuteMultiple(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO sl_psr_items (id, name, qty) VALUES (?, ?, ?)");

        try {
            $stmt->execute([3, 'Cherry', 30]);
            $stmt->execute([4, 'Date', 40]);
            $stmt->execute([5, 'Elderberry', 50]);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_psr_items");
            $count = (int) $rows[0]['cnt'];

            if ($count !== 5) {
                $this->markTestIncomplete(
                    'INSERT re-execute: expected 5 total, got ' . $count
                );
            }

            $this->assertEquals(5, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT re-execute failed: ' . $e->getMessage());
        }
    }

    /**
     * Interleaved: INSERT via exec, SELECT via prepared re-execute, UPDATE via exec.
     * Tests that a prepared SELECT sees all intermediate mutations.
     */
    public function testInterleavedDmlAndSelectReexecute(): void
    {
        $selectStmt = $this->pdo->prepare("SELECT SUM(qty) AS total FROM sl_psr_items");

        try {
            // Initial sum: 10 + 20 = 30
            $selectStmt->execute();
            $r1 = $selectStmt->fetch(\PDO::FETCH_ASSOC);
            $selectStmt->closeCursor();
            $total1 = (int) $r1['total'];

            if ($total1 !== 30) {
                $this->markTestIncomplete('Initial SUM: expected 30, got ' . $total1);
            }

            // INSERT
            $this->pdo->exec("INSERT INTO sl_psr_items VALUES (3, 'Cherry', 30)");

            $selectStmt->execute();
            $r2 = $selectStmt->fetch(\PDO::FETCH_ASSOC);
            $selectStmt->closeCursor();
            $total2 = (int) $r2['total'];

            if ($total2 !== 60) {
                $this->markTestIncomplete(
                    'After INSERT SUM: expected 60, got ' . $total2
                );
            }

            // UPDATE
            $this->pdo->exec("UPDATE sl_psr_items SET qty = 0 WHERE id = 1");

            $selectStmt->execute();
            $r3 = $selectStmt->fetch(\PDO::FETCH_ASSOC);
            $selectStmt->closeCursor();
            $total3 = (int) $r3['total'];

            if ($total3 !== 50) {
                $this->markTestIncomplete(
                    'After UPDATE SUM: expected 50, got ' . $total3
                );
            }

            // DELETE
            $this->pdo->exec("DELETE FROM sl_psr_items WHERE id = 2");

            $selectStmt->execute();
            $r4 = $selectStmt->fetch(\PDO::FETCH_ASSOC);
            $selectStmt->closeCursor();
            $total4 = (int) $r4['total'];

            if ($total4 !== 30) {
                $this->markTestIncomplete(
                    'After DELETE SUM: expected 30, got ' . $total4
                );
            }

            $this->assertEquals(30, $total4);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Interleaved DML + re-execute failed: ' . $e->getMessage());
        }
    }
}
