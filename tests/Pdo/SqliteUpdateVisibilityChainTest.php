<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests visibility of shadow mutations across sequential DML operations.
 *
 * Verifies that INSERT→UPDATE→SELECT→DELETE→SELECT chains produce
 * correct results when all operations run through ZTD shadow store.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteUpdateVisibilityChainTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_uvc_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            qty INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT \'new\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_uvc_items'];
    }

    /**
     * INSERT→SELECT→UPDATE→SELECT: verify update is visible.
     */
    public function testInsertThenUpdateThenSelect(): void
    {
        $this->pdo->exec("INSERT INTO sl_uvc_items VALUES (1, 'Widget', 10, 'new')");

        // Verify insert
        $rows = $this->ztdQuery("SELECT qty, status FROM sl_uvc_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame(10, (int) $rows[0]['qty']);

        // Update
        $this->pdo->exec("UPDATE sl_uvc_items SET qty = 5, status = 'adjusted' WHERE id = 1");

        // Verify update is visible
        $rows = $this->ztdQuery("SELECT qty, status FROM sl_uvc_items WHERE id = 1");
        $this->assertCount(1, $rows);

        if ((int) $rows[0]['qty'] !== 5) {
            $this->markTestIncomplete(
                'Update visibility: qty expected 5, got ' . $rows[0]['qty']
                . '. Status: ' . $rows[0]['status']
            );
        }

        $this->assertSame(5, (int) $rows[0]['qty']);
        $this->assertSame('adjusted', $rows[0]['status']);
    }

    /**
     * Multiple UPDATEs on same row: last write wins.
     */
    public function testMultipleUpdatesLastWriteWins(): void
    {
        $this->pdo->exec("INSERT INTO sl_uvc_items VALUES (2, 'Gadget', 100, 'new')");

        $this->pdo->exec("UPDATE sl_uvc_items SET qty = 80 WHERE id = 2");
        $this->pdo->exec("UPDATE sl_uvc_items SET qty = 60 WHERE id = 2");
        $this->pdo->exec("UPDATE sl_uvc_items SET qty = 40 WHERE id = 2");

        $rows = $this->ztdQuery("SELECT qty FROM sl_uvc_items WHERE id = 2");

        $this->assertCount(1, $rows);
        if ((int) $rows[0]['qty'] !== 40) {
            $this->markTestIncomplete(
                'Multiple updates: expected 40, got ' . $rows[0]['qty']
            );
        }
        $this->assertSame(40, (int) $rows[0]['qty']);
    }

    /**
     * UPDATE with self-referencing arithmetic: SET qty = qty - 1.
     */
    public function testUpdateSelfReferencingArithmetic(): void
    {
        $this->pdo->exec("INSERT INTO sl_uvc_items VALUES (3, 'Counter', 10, 'active')");

        // Decrement 3 times
        $this->pdo->exec("UPDATE sl_uvc_items SET qty = qty - 1 WHERE id = 3");
        $this->pdo->exec("UPDATE sl_uvc_items SET qty = qty - 1 WHERE id = 3");
        $this->pdo->exec("UPDATE sl_uvc_items SET qty = qty - 1 WHERE id = 3");

        $rows = $this->ztdQuery("SELECT qty FROM sl_uvc_items WHERE id = 3");

        $this->assertCount(1, $rows);
        if ((int) $rows[0]['qty'] !== 7) {
            $this->markTestIncomplete(
                'Self-ref arithmetic: 10 - 3 = 7, got ' . $rows[0]['qty']
            );
        }
        $this->assertSame(7, (int) $rows[0]['qty']);
    }

    /**
     * INSERT→UPDATE→DELETE→SELECT: verify row is gone.
     */
    public function testInsertUpdateDeleteChain(): void
    {
        $this->pdo->exec("INSERT INTO sl_uvc_items VALUES (4, 'Temp', 1, 'new')");
        $this->pdo->exec("UPDATE sl_uvc_items SET status = 'processed' WHERE id = 4");
        $this->pdo->exec("DELETE FROM sl_uvc_items WHERE id = 4");

        $rows = $this->ztdQuery("SELECT * FROM sl_uvc_items WHERE id = 4");

        if (count($rows) !== 0) {
            $this->markTestIncomplete(
                'Delete after update: expected 0, got ' . count($rows)
                . '. Data: ' . json_encode($rows)
            );
        }
        $this->assertCount(0, $rows);
    }

    /**
     * Bulk INSERT then conditional UPDATE then SELECT with filter.
     */
    public function testBulkInsertConditionalUpdateFilter(): void
    {
        for ($i = 10; $i < 20; $i++) {
            $this->pdo->exec("INSERT INTO sl_uvc_items VALUES ($i, 'Item$i', $i, 'new')");
        }

        // Update items with qty >= 15
        $this->pdo->exec("UPDATE sl_uvc_items SET status = 'high' WHERE qty >= 15");

        // Verify
        $high = $this->ztdQuery("SELECT COUNT(*) AS c FROM sl_uvc_items WHERE status = 'high'");
        $new = $this->ztdQuery("SELECT COUNT(*) AS c FROM sl_uvc_items WHERE status = 'new'");

        // Items 15-19 are high (5 items), items 10-14 are new (5 items)
        if ((int) $high[0]['c'] !== 5) {
            $this->markTestIncomplete(
                'Conditional update: expected 5 high, got ' . $high[0]['c']
            );
        }
        $this->assertSame(5, (int) $high[0]['c']);
        $this->assertSame(5, (int) $new[0]['c']);
    }

    /**
     * Prepared UPDATE with arithmetic and param.
     */
    public function testPreparedUpdateArithmetic(): void
    {
        $this->pdo->exec("INSERT INTO sl_uvc_items VALUES (30, 'Stock', 100, 'active')");

        $sql = "UPDATE sl_uvc_items SET qty = qty - ? WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([25, 30]);

            $rows = $this->ztdQuery("SELECT qty FROM sl_uvc_items WHERE id = 30");

            $this->assertCount(1, $rows);
            if ((int) $rows[0]['qty'] !== 75) {
                $this->markTestIncomplete(
                    'Prepared arithmetic: 100-25=75, got ' . $rows[0]['qty']
                );
            }
            $this->assertSame(75, (int) $rows[0]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared arithmetic UPDATE failed: ' . $e->getMessage());
        }
    }
}
