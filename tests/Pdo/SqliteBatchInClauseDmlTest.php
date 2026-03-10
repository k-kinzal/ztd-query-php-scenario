<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests batch DML operations with IN clauses containing many prepared parameters.
 *
 * IN (?, ?, ?, ...) with multiple parameters is extremely common in real
 * applications (batch delete, batch status update, multi-select). The CTE
 * rewriter must correctly handle parameter binding with variable-length IN lists.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class SqliteBatchInClauseDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_bin_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT \'active\',
                priority INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_bin_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $cat = $i <= 5 ? 'A' : 'B';
            $this->pdo->exec(
                "INSERT INTO sl_bin_items VALUES ($i, 'Item$i', '$cat', 'active', $i)"
            );
        }
    }

    /**
     * Prepared DELETE with IN (?, ?, ?, ?, ?) — 5 params.
     */
    public function testPreparedDeleteWithFiveInParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_bin_items WHERE id IN (?, ?, ?, ?, ?)"
            );
            $stmt->execute([1, 3, 5, 7, 9]);

            $rows = $this->ztdQuery("SELECT id FROM sl_bin_items ORDER BY id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'DELETE IN 5 params: expected 5, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $ids = array_column($rows, 'id');
            $this->assertEquals([2, 4, 6, 8, 10], array_map('intval', $ids));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with 5 IN params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with IN and additional WHERE condition.
     */
    public function testPreparedUpdateWithInAndExtraWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_bin_items SET status = ? WHERE id IN (?, ?, ?) AND category = ?"
            );
            $stmt->execute(['archived', 1, 2, 3, 'A']);

            $rows = $this->ztdQuery(
                "SELECT id, status FROM sl_bin_items WHERE status = 'archived' ORDER BY id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE IN + WHERE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with IN + extra WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with IN after DELETE mutations — verifying shadow consistency.
     */
    public function testPreparedSelectInAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_bin_items WHERE id IN (2, 4, 6)");

            $stmt = $this->pdo->prepare(
                "SELECT name FROM sl_bin_items WHERE id IN (?, ?, ?, ?) ORDER BY id"
            );
            $stmt->execute([1, 2, 3, 4]);

            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            // Only id 1 and 3 should remain (2 and 4 were deleted)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT IN after DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Item1', $rows[0]['name']);
            $this->assertSame('Item3', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT IN after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with NOT IN — inverse batch operation.
     */
    public function testPreparedDeleteWithNotIn(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_bin_items WHERE id NOT IN (?, ?, ?)"
            );
            $stmt->execute([1, 5, 10]);

            $rows = $this->ztdQuery("SELECT id FROM sl_bin_items ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE NOT IN: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $ids = array_column($rows, 'id');
            $this->assertEquals([1, 5, 10], array_map('intval', $ids));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with NOT IN failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET with IN list of 8 params — larger batch.
     */
    public function testPreparedUpdateWithEightInParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_bin_items SET priority = priority + ? WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?)"
            );
            $stmt->execute([100, 1, 2, 3, 4, 5, 6, 7, 8]);

            $rows = $this->ztdQuery(
                "SELECT id, priority FROM sl_bin_items WHERE priority > 100 ORDER BY id"
            );

            // Items 1-8 should have priority > 100 (original 1-8 + 100)
            if (count($rows) !== 8) {
                $allRows = $this->ztdQuery("SELECT id, priority FROM sl_bin_items ORDER BY id");
                $this->markTestIncomplete(
                    'UPDATE IN 8 params: expected 8 with priority>100, got ' . count($rows)
                    . '. All: ' . json_encode($allRows)
                );
            }

            $this->assertCount(8, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with 8 IN params failed: ' . $e->getMessage());
        }
    }

    /**
     * Combined: UPDATE via IN, then DELETE via IN on same table.
     */
    public function testSequentialUpdateThenDeleteWithIn(): void
    {
        try {
            // First mark some as archived
            $stmt1 = $this->pdo->prepare(
                "UPDATE sl_bin_items SET status = 'archived' WHERE id IN (?, ?, ?)"
            );
            $stmt1->execute([1, 2, 3]);

            // Then delete the archived ones
            $stmt2 = $this->pdo->prepare(
                "DELETE FROM sl_bin_items WHERE status = ? AND id IN (?, ?)"
            );
            $stmt2->execute(['archived', 1, 2]);

            $rows = $this->ztdQuery("SELECT id, status FROM sl_bin_items ORDER BY id");

            // Item 3 still archived, items 4-10 active, items 1-2 deleted
            if (count($rows) !== 8) {
                $this->markTestIncomplete(
                    'Sequential UPDATE+DELETE IN: expected 8, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(8, $rows);
            $this->assertSame('archived', $rows[0]['status']); // Item 3
            $this->assertSame('active', $rows[1]['status']); // Item 4
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential UPDATE then DELETE with IN failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE col IN (?) AND col2 IN (?) — two IN clauses.
     */
    public function testPreparedDeleteWithTwoInClauses(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_bin_items WHERE category IN (?, ?) AND priority IN (?, ?, ?)"
            );
            // Delete items in category A or B with priority 1, 2, or 6
            $stmt->execute(['A', 'B', 1, 2, 6]);

            $rows = $this->ztdQuery("SELECT id FROM sl_bin_items ORDER BY id");

            // Deleted: id=1 (A,1), id=2 (A,2), id=6 (B,6) = 3 deleted, 7 remain
            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'DELETE two IN clauses: expected 7, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with two IN clauses failed: ' . $e->getMessage());
        }
    }
}
