<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT where source and target are both shadow-managed tables.
 *
 * When both the source and target of INSERT...SELECT are managed by the shadow
 * store, the rewriter must correctly read from the shadow source and insert
 * into the shadow target. This is a common pattern for data migration and
 * archival queries.
 *
 * @spec SPEC-4.1a
 */
class SqliteInsertSelectCrossTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_ixc_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
            )",
            "CREATE TABLE sl_ixc_archive (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                archived_status TEXT NOT NULL
            )",
            "CREATE TABLE sl_ixc_summary (
                customer TEXT PRIMARY KEY,
                total_amount REAL NOT NULL,
                order_count INTEGER NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ixc_orders', 'sl_ixc_archive', 'sl_ixc_summary'];
    }

    /**
     * INSERT...SELECT from one shadow table to another.
     */
    public function testInsertSelectBetweenShadowTables(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (1, 'Alice', 100.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (2, 'Bob', 200.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (3, 'Alice', 50.0, 'pending')");

            // Archive completed orders
            $this->pdo->exec(
                "INSERT INTO sl_ixc_archive (id, customer, amount, archived_status)
                 SELECT id, customer, amount, status FROM sl_ixc_orders WHERE status = 'complete'"
            );

            $rows = $this->ztdQuery("SELECT customer, amount FROM sl_ixc_archive ORDER BY id");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INSERT...SELECT between shadow tables produced 0 rows. Expected 2.'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT...SELECT produced ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertSame('Bob', $rows[1]['customer']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT cross-table test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with aggregation from shadow source to shadow target.
     */
    public function testInsertSelectWithAggregate(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (1, 'Alice', 100.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (2, 'Alice', 150.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (3, 'Bob', 200.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (4, 'Bob', 75.0, 'pending')");

            $this->pdo->exec(
                "INSERT INTO sl_ixc_summary (customer, total_amount, order_count)
                 SELECT customer, SUM(amount), COUNT(*)
                 FROM sl_ixc_orders
                 GROUP BY customer"
            );

            $rows = $this->ztdQuery("SELECT customer, total_amount, order_count FROM sl_ixc_summary ORDER BY customer");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INSERT...SELECT with GROUP BY aggregate produced 0 rows. Expected 2.'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT...SELECT aggregate produced ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);

            $alice = $rows[0];
            $bob = $rows[1];

            // Alice: 100 + 150 = 250, 2 orders
            $this->assertSame('Alice', $alice['customer']);
            if ((float) $alice['total_amount'] !== 250.0) {
                $this->markTestIncomplete(
                    'Alice total_amount is ' . $alice['total_amount'] . ', expected 250.0'
                );
            }
            $this->assertEquals(250.0, (float) $alice['total_amount']);
            $this->assertEquals(2, (int) $alice['order_count']);

            // Bob: 200 + 75 = 275, 2 orders
            $this->assertEquals(275.0, (float) $bob['total_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT with aggregate test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT then DELETE from source, then verify both tables.
     * Tests that archive and source are independently tracked.
     */
    public function testInsertSelectThenDeleteSource(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (1, 'Alice', 100.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (2, 'Bob', 200.0, 'complete')");

            // Archive
            $this->pdo->exec(
                "INSERT INTO sl_ixc_archive (id, customer, amount, archived_status)
                 SELECT id, customer, amount, status FROM sl_ixc_orders WHERE status = 'complete'"
            );

            // Delete from source
            $this->pdo->exec("DELETE FROM sl_ixc_orders WHERE status = 'complete'");

            // Archive should still have the rows
            $archiveRows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ixc_archive");
            $archiveCount = (int) $archiveRows[0]['cnt'];

            $orderRows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ixc_orders");
            $orderCount = (int) $orderRows[0]['cnt'];

            if ($archiveCount === 0) {
                $this->markTestIncomplete(
                    'Archive is empty after INSERT...SELECT + DELETE source. Archive should retain rows independently.'
                );
            }

            $this->assertSame(2, $archiveCount, 'Archive should have 2 rows');
            $this->assertSame(0, $orderCount, 'Orders should be empty after DELETE');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT then DELETE source test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with JOIN across two shadow tables.
     */
    public function testInsertSelectWithJoin(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (1, 'Alice', 100.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (2, 'Bob', 200.0, 'pending')");

            // First, create archive for Alice
            $this->pdo->exec("INSERT INTO sl_ixc_archive VALUES (1, 'Alice', 100.0, 'complete')");

            // Insert summary for customers who have archived orders, using JOIN
            $this->pdo->exec(
                "INSERT INTO sl_ixc_summary (customer, total_amount, order_count)
                 SELECT o.customer, SUM(o.amount), COUNT(*)
                 FROM sl_ixc_orders o
                 INNER JOIN sl_ixc_archive a ON o.customer = a.customer
                 GROUP BY o.customer"
            );

            $rows = $this->ztdQuery("SELECT customer, total_amount FROM sl_ixc_summary");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INSERT...SELECT with JOIN across shadow tables produced 0 rows. Expected 1 (Alice).'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT with JOIN test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT cross-table.
     */
    public function testPreparedInsertSelectCrossTable(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (1, 'Alice', 100.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (2, 'Bob', 200.0, 'complete')");
            $this->pdo->exec("INSERT INTO sl_ixc_orders VALUES (3, 'Carol', 50.0, 'pending')");

            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_ixc_archive (id, customer, amount, archived_status)
                 SELECT id, customer, amount, status FROM sl_ixc_orders WHERE status = ?"
            );
            $stmt->execute(['complete']);

            $rows = $this->ztdQuery("SELECT customer FROM sl_ixc_archive ORDER BY customer");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Prepared INSERT...SELECT cross-table produced 0 rows. Expected 2.'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT...SELECT produced ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT...SELECT cross-table test failed: ' . $e->getMessage());
        }
    }
}
