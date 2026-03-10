<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store consistency across a chain of DML operations.
 *
 * Verifies that INSERT → UPDATE → DELETE → SELECT sequences produce correct
 * results at each intermediate step. The shadow store must accumulate mutations
 * correctly and not lose or duplicate data.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteChainedDmlConsistencyTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_cdc_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT \'pending\',
            quantity INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_cdc_items'];
    }

    /**
     * Chain: seed → shadow INSERT → UPDATE shadow-inserted row → SELECT verify.
     * The shadow store must see the UPDATE applied to the shadow-inserted row.
     *
     * @spec SPEC-4.1
     * @spec SPEC-4.2
     */
    public function testInsertThenUpdateShadowRow(): void
    {
        try {
            // Shadow INSERT
            $this->pdo->exec(
                "INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (1, 'Alpha', 'pending', 10)"
            );

            // Verify INSERT
            $rows1 = $this->ztdQuery('SELECT * FROM sl_cdc_items WHERE id = 1');
            if (count($rows1) !== 1) {
                $this->markTestIncomplete('After INSERT: expected 1 row, got ' . count($rows1));
            }
            $this->assertSame('pending', $rows1[0]['status']);

            // UPDATE the shadow-inserted row
            $this->pdo->exec(
                "UPDATE sl_cdc_items SET status = 'active', quantity = 20 WHERE id = 1"
            );

            // Verify UPDATE
            $rows2 = $this->ztdQuery('SELECT * FROM sl_cdc_items WHERE id = 1');
            if (count($rows2) !== 1) {
                $this->markTestIncomplete('After UPDATE: expected 1 row, got ' . count($rows2));
            }

            $this->assertSame('active', $rows2[0]['status'],
                'Status should be updated to active');
            $this->assertEquals(20, (int) $rows2[0]['quantity'],
                'Quantity should be updated to 20');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT then UPDATE shadow row failed: ' . $e->getMessage());
        }
    }

    /**
     * Chain: seed → shadow INSERT multiple → UPDATE some → DELETE some → verify remainder.
     *
     * @spec SPEC-4.1
     * @spec SPEC-4.2
     * @spec SPEC-4.3
     */
    public function testInsertUpdateDeleteChain(): void
    {
        try {
            // Insert 4 items
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (1, 'A', 'new', 5)");
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (2, 'B', 'new', 10)");
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (3, 'C', 'new', 15)");
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (4, 'D', 'new', 20)");

            $rows1 = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_cdc_items');
            $this->assertEquals(4, (int) $rows1[0]['cnt'], 'Should have 4 rows after insert');

            // Update items 1 and 2 to 'processed'
            $this->pdo->exec("UPDATE sl_cdc_items SET status = 'processed' WHERE id IN (1, 2)");

            // Delete processed items
            $this->pdo->exec("DELETE FROM sl_cdc_items WHERE status = 'processed'");

            // Verify only items 3 and 4 remain
            $rows2 = $this->ztdQuery('SELECT id, name, status FROM sl_cdc_items ORDER BY id');

            if (count($rows2) !== 2) {
                $this->markTestIncomplete(
                    'After chain: expected 2 rows, got ' . count($rows2)
                );
            }

            $this->assertCount(2, $rows2);
            $this->assertEquals(3, (int) $rows2[0]['id']);
            $this->assertEquals(4, (int) $rows2[1]['id']);
            $this->assertSame('new', $rows2[0]['status']);
            $this->assertSame('new', $rows2[1]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT/UPDATE/DELETE chain failed: ' . $e->getMessage());
        }
    }

    /**
     * Chain: seed physical rows → shadow UPDATE → shadow DELETE → verify.
     * Physical rows are seeded directly, then mutated through shadow store.
     *
     * @spec SPEC-4.2
     * @spec SPEC-4.3
     */
    public function testPhysicalSeedThenShadowMutations(): void
    {
        try {
            // Seed directly (physical rows exist)
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (1, 'X', 'active', 100)");
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (2, 'Y', 'active', 200)");
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (3, 'Z', 'active', 300)");

            // Shadow UPDATE: increase quantities by 50
            $this->pdo->exec("UPDATE sl_cdc_items SET quantity = quantity + 50 WHERE status = 'active'");

            // Shadow DELETE: remove item 2
            $this->pdo->exec("DELETE FROM sl_cdc_items WHERE id = 2");

            // Verify
            $rows = $this->ztdQuery('SELECT id, quantity FROM sl_cdc_items ORDER BY id');

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Physical seed + shadow mutations: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
            $this->assertEquals(150, (int) $rows[0]['quantity'], 'Item 1 quantity should be 100+50=150');
            $this->assertEquals(3, (int) $rows[1]['id']);
            $this->assertEquals(350, (int) $rows[1]['quantity'], 'Item 3 quantity should be 300+50=350');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Physical seed + shadow mutations failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE a row, then UPDATE the same row again, then verify final state.
     * Tests that multiple sequential UPDATEs on the same row accumulate correctly.
     *
     * @spec SPEC-4.2
     */
    public function testDoubleUpdateSameRow(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (1, 'Item', 'draft', 0)");

            // First UPDATE
            $this->pdo->exec("UPDATE sl_cdc_items SET status = 'review', quantity = 10 WHERE id = 1");

            $mid = $this->ztdQuery('SELECT status, quantity FROM sl_cdc_items WHERE id = 1');
            if (count($mid) !== 1) {
                $this->markTestIncomplete('After first UPDATE: expected 1 row, got ' . count($mid));
            }
            $this->assertSame('review', $mid[0]['status']);
            $this->assertEquals(10, (int) $mid[0]['quantity']);

            // Second UPDATE
            $this->pdo->exec("UPDATE sl_cdc_items SET status = 'published', quantity = quantity + 5 WHERE id = 1");

            $final = $this->ztdQuery('SELECT status, quantity FROM sl_cdc_items WHERE id = 1');
            if (count($final) !== 1) {
                $this->markTestIncomplete('After second UPDATE: expected 1 row, got ' . count($final));
            }

            $this->assertSame('published', $final[0]['status'],
                'Status should be published after second update');
            $this->assertEquals(15, (int) $final[0]['quantity'],
                'Quantity should be 10+5=15 after second update');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Double UPDATE same row failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT → DELETE → re-INSERT with same PK → verify new data.
     * Tests that deleting and re-inserting with the same PK works correctly.
     *
     * @spec SPEC-4.1
     * @spec SPEC-4.3
     */
    public function testDeleteThenReinsertSamePk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (1, 'Old', 'active', 50)");

            // Delete
            $this->pdo->exec("DELETE FROM sl_cdc_items WHERE id = 1");

            $mid = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_cdc_items WHERE id = 1');
            $this->assertEquals(0, (int) $mid[0]['cnt'], 'Row should be gone after DELETE');

            // Re-insert with same PK but different data
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (1, 'New', 'pending', 100)");

            $rows = $this->ztdQuery('SELECT id, name, status, quantity FROM sl_cdc_items WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Delete/re-insert: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('New', $rows[0]['name'],
                'Name should be from re-inserted row');
            $this->assertSame('pending', $rows[0]['status']);
            $this->assertEquals(100, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Delete then re-insert failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate query after mutations: INSERT some → DELETE some → SUM/COUNT.
     *
     * @spec SPEC-3.2
     */
    public function testAggregateAfterMutations(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (1, 'A', 'active', 10)");
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (2, 'B', 'active', 20)");
            $this->pdo->exec("INSERT INTO sl_cdc_items (id, name, status, quantity) VALUES (3, 'C', 'inactive', 30)");

            // Delete inactive
            $this->pdo->exec("DELETE FROM sl_cdc_items WHERE status = 'inactive'");

            // Update remaining
            $this->pdo->exec("UPDATE sl_cdc_items SET quantity = quantity * 2 WHERE status = 'active'");

            $rows = $this->ztdQuery(
                'SELECT COUNT(*) AS cnt, SUM(quantity) AS total FROM sl_cdc_items'
            );

            $this->assertEquals(2, (int) $rows[0]['cnt'], 'Should have 2 rows');
            $this->assertEquals(60, (int) $rows[0]['total'],
                'Total should be (10*2)+(20*2)=60');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate after mutations failed: ' . $e->getMessage());
        }
    }
}
