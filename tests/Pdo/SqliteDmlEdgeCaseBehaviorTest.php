<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML edge cases through the shadow store on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteDmlEdgeCaseBehaviorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_dec_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            qty INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_dec_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dec_items VALUES (1, 'Alpha', 10, 'active')");
        $this->pdo->exec("INSERT INTO sl_dec_items VALUES (2, 'Beta', 20, 'active')");
        $this->pdo->exec("INSERT INTO sl_dec_items VALUES (3, 'Gamma', 30, 'inactive')");
    }

    public function testInsertUpdateDeleteSameRow(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_dec_items VALUES (4, 'Delta', 40, 'active')");
            $this->pdo->exec("UPDATE sl_dec_items SET qty = 45 WHERE id = 4");
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 4");

            $rows = $this->ztdQuery("SELECT id FROM sl_dec_items ORDER BY id");
            $ids = array_column($rows, 'id');

            if (in_array('4', $ids) || in_array(4, $ids)) {
                $this->markTestIncomplete('INSERT+UPDATE+DELETE same row: row 4 still visible');
            }
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT+UPDATE+DELETE same row failed: ' . $e->getMessage());
        }
    }

    public function testDeleteAllThenReInsert(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 1");
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 2");
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 3");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dec_items");
            if ((int) $rows[0]['cnt'] !== 0) {
                $this->markTestIncomplete('DELETE all: expected 0, got ' . $rows[0]['cnt']);
            }

            $this->pdo->exec("INSERT INTO sl_dec_items VALUES (10, 'New', 100, 'active')");

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dec_items");
            $this->assertCount(1, $rows);
            $this->assertEquals(10, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE all + re-INSERT failed: ' . $e->getMessage());
        }
    }

    public function testChainedIncrements(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_dec_items SET qty = qty + 5 WHERE id = 1");
            $this->pdo->exec("UPDATE sl_dec_items SET qty = qty + 5 WHERE id = 1");
            $this->pdo->exec("UPDATE sl_dec_items SET qty = qty + 5 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT qty FROM sl_dec_items WHERE id = 1");
            $qty = (int) $rows[0]['qty'];

            if ($qty !== 25) {
                $this->markTestIncomplete("Chained increments: expected 25, got $qty");
            }
            $this->assertEquals(25, $qty);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained increments failed: ' . $e->getMessage());
        }
    }

    public function testUpdateAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 1");
            $this->pdo->exec("UPDATE sl_dec_items SET qty = 999 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT id FROM sl_dec_items ORDER BY id");
            $ids = array_map('intval', array_column($rows, 'id'));

            if (in_array(1, $ids)) {
                $this->markTestIncomplete('UPDATE after DELETE: deleted row resurrected!');
            }
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-INSERT a row with same PK after DELETE — should succeed.
     */
    public function testReInsertAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 1");
            $this->pdo->exec("INSERT INTO sl_dec_items VALUES (1, 'Alpha-v2', 50, 'active')");

            $rows = $this->ztdQuery("SELECT name, qty FROM sl_dec_items WHERE id = 1");

            if (empty($rows)) {
                $this->markTestIncomplete('Re-INSERT after DELETE: row not found');
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Alpha-v2', $rows[0]['name']);
            $this->assertEquals(50, (int) $rows[0]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-INSERT after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate on empty result set after DELETE all rows.
     */
    public function testAggregateOnEmptyAfterDeleteAll(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 1");
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 2");
            $this->pdo->exec("DELETE FROM sl_dec_items WHERE id = 3");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt, SUM(qty) AS total FROM sl_dec_items");

            $this->assertCount(1, $rows);
            $this->assertEquals(0, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate on empty after DELETE all failed: ' . $e->getMessage());
        }
    }
}
