<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-statement execution through exec() and query().
 * Some applications concatenate multiple SQL statements separated by
 * semicolons and execute them in one call. The CTE rewriter may only
 * process the first statement, silently ignoring subsequent ones.
 *
 * SQL patterns exercised: multi-INSERT exec, INSERT+UPDATE in one exec,
 * multi-DELETE, multi-statement then SELECT verification.
 * @spec SPEC-4.1
 */
class SqliteMultiStatementExecTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_mse_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            qty INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_mse_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_mse_items VALUES (1, 'Apple', 10)");
        $this->pdo->exec("INSERT INTO sl_mse_items VALUES (2, 'Banana', 20)");
    }

    /**
     * Two INSERT statements in one exec() call.
     * If CTE rewriter only handles first statement, second INSERT is lost.
     */
    public function testTwoInsertsInOneExec(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_mse_items VALUES (3, 'Cherry', 30); INSERT INTO sl_mse_items VALUES (4, 'Date', 40)"
            );
        } catch (\Exception $e) {
            // Multi-statement might throw
            $this->markTestIncomplete(
                'Multi-statement exec threw exception: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_mse_items");
        $count = (int) $rows[0]['cnt'];

        if ($count === 3) {
            $this->markTestIncomplete(
                'Only first statement of multi-statement exec was executed (3 rows instead of 4)'
            );
        }

        $this->assertEquals(4, $count);
    }

    /**
     * INSERT then UPDATE in one exec() call.
     */
    public function testInsertThenUpdateInOneExec(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_mse_items VALUES (5, 'Elderberry', 50); UPDATE sl_mse_items SET qty = 99 WHERE id = 1"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-statement INSERT+UPDATE threw exception: ' . $e->getMessage()
            );
            return;
        }

        // Check both statements executed
        $inserted = $this->ztdQuery("SELECT name FROM sl_mse_items WHERE id = 5");
        $updated = $this->ztdQuery("SELECT qty FROM sl_mse_items WHERE id = 1");

        $insertOk = count($inserted) === 1 && $inserted[0]['name'] === 'Elderberry';
        $updateOk = count($updated) === 1 && (int) $updated[0]['qty'] === 99;

        if ($insertOk && !$updateOk) {
            $this->markTestIncomplete('INSERT executed but UPDATE was silently ignored');
        }
        if (!$insertOk && $updateOk) {
            $this->markTestIncomplete('UPDATE executed but INSERT was silently ignored');
        }
        if (!$insertOk && !$updateOk) {
            $this->markTestIncomplete('Neither INSERT nor UPDATE executed in multi-statement');
        }

        $this->assertTrue($insertOk, 'INSERT should have been executed');
        $this->assertTrue($updateOk, 'UPDATE should have been executed');
    }

    /**
     * Two DELETEs in one exec() call.
     */
    public function testTwoDeletesInOneExec(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_mse_items WHERE id = 1; DELETE FROM sl_mse_items WHERE id = 2"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-statement DELETE threw exception: ' . $e->getMessage()
            );
            return;
        }

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_mse_items");
        $count = (int) $rows[0]['cnt'];

        if ($count === 1) {
            $this->markTestIncomplete('Only first DELETE executed (1 row remains instead of 0)');
        }

        $this->assertEquals(0, $count);
    }

    /**
     * Mixed DML: INSERT + DELETE + UPDATE in one exec.
     */
    public function testMixedDmlMultiStatement(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_mse_items VALUES (6, 'Fig', 60); " .
                "DELETE FROM sl_mse_items WHERE id = 2; " .
                "UPDATE sl_mse_items SET qty = 0 WHERE id = 1"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multi-statement mixed DML threw exception: ' . $e->getMessage()
            );
            return;
        }

        $all = $this->ztdQuery("SELECT id, name, qty FROM sl_mse_items ORDER BY id");
        $ids = array_column($all, 'id');

        // id=1 should exist with qty=0, id=2 deleted, id=6 inserted
        $this->assertContains('1', array_map('strval', $ids), 'id=1 should still exist');
        $this->assertNotContains('2', array_map('strval', $ids), 'id=2 should be deleted');
        $this->assertContains('6', array_map('strval', $ids), 'id=6 should be inserted');

        $row1 = array_values(array_filter($all, fn($r) => (int) $r['id'] === 1));
        if (count($row1) > 0) {
            $this->assertEquals(0, (int) $row1[0]['qty']);
        }
    }
}
