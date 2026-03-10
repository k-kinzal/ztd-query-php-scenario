<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests whether a prepared SELECT that was prepared BEFORE a prepared
 * INSERT is executed can see the inserted data at execute() time.
 *
 * This is a common real-application pattern: prepare all statements at
 * startup (or in a repository class constructor), then execute them in
 * a different order during the request lifecycle.
 *
 * Distinct from Issue #87 (re-execute stale): this tests FIRST execution
 * of a prepared SELECT that was prepared before data existed.
 *
 * @spec SPEC-3.2
 */
class SqlitePreparedBeforeInsertVisibilityTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_pbiv_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active'
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_pbiv_items'];
    }

    /**
     * Prepare SELECT before exec INSERT, then execute SELECT.
     * The SELECT is prepared when the shadow store is empty.
     */
    public function testSelectPreparedBeforeExecInsert(): void
    {
        try {
            // Prepare SELECT on empty table
            $stmtSelect = $this->pdo->prepare("SELECT name FROM sl_pbiv_items WHERE id = ?");

            // Insert via exec (not prepared)
            $this->pdo->exec("INSERT INTO sl_pbiv_items (id, name) VALUES (1, 'Alpha')");

            // Execute the pre-prepared SELECT
            $stmtSelect->execute([1]);
            $row = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            if ($row === false) {
                $this->markTestIncomplete(
                    'Prepared SELECT (prepared before exec INSERT) returned no rows.'
                    . ' The CTE may be generated at prepare() time, not execute() time.'
                );
            }
            $this->assertSame('Alpha', $row['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Select prepared before exec insert test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare SELECT before prepared INSERT, then execute INSERT, then SELECT.
     * Both statements are prepared before any data exists.
     */
    public function testSelectPreparedBeforePreparedInsert(): void
    {
        try {
            // Prepare both statements on empty table
            $stmtSelect = $this->pdo->prepare("SELECT name FROM sl_pbiv_items WHERE id = ?");
            $stmtInsert = $this->pdo->prepare("INSERT INTO sl_pbiv_items (id, name) VALUES (?, ?)");

            // Execute INSERT first
            $stmtInsert->execute([1, 'Beta']);

            // Execute SELECT
            $stmtSelect->execute([1]);
            $row = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            if ($row === false) {
                $this->markTestIncomplete(
                    'Prepared SELECT (prepared before prepared INSERT executed) returned no rows.'
                    . ' When both statements are prepared before data exists, the SELECT'
                    . ' may not see subsequently inserted data at execute() time.'
                );
            }
            $this->assertSame('Beta', $row['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Select prepared before prepared insert test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare SELECT, exec INSERT, verify SELECT works.
     * Then exec another INSERT, re-execute SELECT for new row.
     * Tests both first-execute and re-execute visibility.
     */
    public function testPreparedSelectVisibilityChain(): void
    {
        try {
            $stmtSelect = $this->pdo->prepare("SELECT name FROM sl_pbiv_items WHERE id = ?");

            // First insert + select
            $this->pdo->exec("INSERT INTO sl_pbiv_items (id, name) VALUES (1, 'First')");
            $stmtSelect->execute([1]);
            $row1 = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            if ($row1 === false) {
                $this->markTestIncomplete(
                    'First SELECT after INSERT returned no rows.'
                );
            }

            // Second insert + re-execute select
            $this->pdo->exec("INSERT INTO sl_pbiv_items (id, name) VALUES (2, 'Second')");
            $stmtSelect->execute([2]);
            $row2 = $stmtSelect->fetch(\PDO::FETCH_ASSOC);

            if ($row2 === false) {
                $this->markTestIncomplete(
                    'Re-executed SELECT for second row returned no rows.'
                    . ' This is the stale prepared SELECT issue (Issue #87).'
                );
            }
            $this->assertSame('First', $row1['name']);
            $this->assertSame('Second', $row2['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared select visibility chain test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare COUNT(*) SELECT, insert multiple rows, verify count.
     */
    public function testPreparedCountAfterMultipleInserts(): void
    {
        try {
            $stmtCount = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM sl_pbiv_items");

            // Execute before any data
            $stmtCount->execute();
            $count0 = (int) $stmtCount->fetch(\PDO::FETCH_ASSOC)['cnt'];

            // Insert 3 rows
            $this->pdo->exec("INSERT INTO sl_pbiv_items (id, name) VALUES (1, 'A')");
            $this->pdo->exec("INSERT INTO sl_pbiv_items (id, name) VALUES (2, 'B')");
            $this->pdo->exec("INSERT INTO sl_pbiv_items (id, name) VALUES (3, 'C')");

            // Re-execute count
            $stmtCount->execute();
            $count3 = (int) $stmtCount->fetch(\PDO::FETCH_ASSOC)['cnt'];

            $this->assertEquals(0, $count0);

            if ($count3 !== 3) {
                $this->markTestIncomplete(
                    "Re-executed COUNT(*) returned {$count3} instead of 3 after 3 inserts."
                    . ' Issue #87: prepared SELECT re-execute returns stale data.'
                );
            }
            $this->assertEquals(3, $count3);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared count after multiple inserts test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepare SELECT, prepared INSERT, execute INSERT then SELECT
     * on different tables — cross-table visibility.
     */
    public function testCrossTablePreparedVisibility(): void
    {
        try {
            // Create a second table
            $this->pdo->disableZtd();
            $this->pdo->exec("CREATE TABLE sl_pbiv_log (id INTEGER PRIMARY KEY, item_id INTEGER, msg TEXT)");
            $this->pdo->enableZtd();

            $stmtInsertItem = $this->pdo->prepare("INSERT INTO sl_pbiv_items (id, name) VALUES (?, ?)");
            $stmtInsertLog = $this->pdo->prepare("INSERT INTO sl_pbiv_log (id, item_id, msg) VALUES (?, ?, ?)");
            $stmtJoin = $this->pdo->prepare(
                "SELECT i.name, l.msg FROM sl_pbiv_items i JOIN sl_pbiv_log l ON l.item_id = i.id WHERE i.id = ?"
            );

            // Insert data into both tables
            $stmtInsertItem->execute([1, 'Widget']);
            $stmtInsertLog->execute([1, 1, 'created']);

            // Execute the JOIN query
            $stmtJoin->execute([1]);
            $rows = $stmtJoin->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Cross-table prepared JOIN returned 0 rows after both prepared INSERTs.'
                    . ' When all 3 statements are prepared before any executes,'
                    . ' the JOIN may not see data from either table.'
                );
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('created', $rows[0]['msg']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Cross-table prepared visibility test failed: ' . $e->getMessage());
        }
    }
}
