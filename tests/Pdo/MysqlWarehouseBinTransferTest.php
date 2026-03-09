<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests warehouse bin transfer patterns: DELETE WHERE LIKE, prepared SELECT with
 * LIKE wildcard, prepared UPDATE with arithmetic self-reference, mixed exec()+prepare()
 * shadow store consistency, INSERT...SELECT from joined tables with expressions,
 * decrement-and-verify, delete+insert shadow coherence, and JOIN with LIKE after
 * shadow operations (MySQL PDO).
 * SQL patterns exercised: DELETE WHERE LIKE, prepared LIKE ?, UPDATE SET col = col + ?,
 * mixed exec/prepare on same table, INSERT INTO...SELECT...JOIN, UPDATE SET col = col - ?.
 * @spec SPEC-10.2.183
 */
class MysqlWarehouseBinTransferTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_wbin_bins (
                id INT PRIMARY KEY,
                location VARCHAR(20),
                capacity INT,
                current_qty INT
            )',
            'CREATE TABLE mp_wbin_items (
                id INT PRIMARY KEY,
                bin_id INT,
                sku VARCHAR(30),
                description VARCHAR(100),
                qty INT
            )',
            'CREATE TABLE mp_wbin_transfers (
                id INT PRIMARY KEY,
                item_id INT,
                from_bin INT,
                to_bin INT,
                qty INT,
                status VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_wbin_transfers', 'mp_wbin_items', 'mp_wbin_bins'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_wbin_bins VALUES (1, 'A-01', 100, 45)");
        $this->pdo->exec("INSERT INTO mp_wbin_bins VALUES (2, 'A-02', 100, 30)");
        $this->pdo->exec("INSERT INTO mp_wbin_bins VALUES (3, 'B-01', 200, 150)");
        $this->pdo->exec("INSERT INTO mp_wbin_bins VALUES (4, 'B-02', 200, 10)");

        $this->pdo->exec("INSERT INTO mp_wbin_items VALUES (1, 1, 'SKU-WIDGET-001', 'Standard Widget', 20)");
        $this->pdo->exec("INSERT INTO mp_wbin_items VALUES (2, 1, 'SKU-WIDGET-002', 'Premium Widget', 15)");
        $this->pdo->exec("INSERT INTO mp_wbin_items VALUES (3, 1, 'TEMP-RECV-001', 'Temporary receiving item', 10)");
        $this->pdo->exec("INSERT INTO mp_wbin_items VALUES (4, 2, 'SKU-GADGET-001', 'Basic Gadget', 30)");
        $this->pdo->exec("INSERT INTO mp_wbin_items VALUES (5, 3, 'SKU-WIDGET-001', 'Standard Widget', 100)");
        $this->pdo->exec("INSERT INTO mp_wbin_items VALUES (6, 3, 'SKU-GADGET-002', 'Advanced Gadget', 50)");
        $this->pdo->exec("INSERT INTO mp_wbin_items VALUES (7, 4, 'TEMP-RECV-002', 'Temporary receiving item', 10)");

        $this->pdo->exec("INSERT INTO mp_wbin_transfers VALUES (1, 1, 1, 3, 5, 'completed')");
        $this->pdo->exec("INSERT INTO mp_wbin_transfers VALUES (2, 4, 2, 3, 10, 'pending')");
    }

    public function testDeleteWhereLike(): void
    {
        $affected = $this->ztdExec("DELETE FROM mp_wbin_items WHERE sku LIKE 'TEMP-%'");

        $this->assertSame(2, $affected);

        $rows = $this->ztdQuery("SELECT id, sku FROM mp_wbin_items ORDER BY id");
        $this->assertCount(5, $rows);

        foreach ($rows as $row) {
            $this->assertStringStartsNotWith('TEMP-', $row['sku']);
        }
    }

    public function testPreparedSelectWithLikeWildcard(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, sku, description FROM mp_wbin_items WHERE sku LIKE ? ORDER BY id",
            ['SKU-WIDGET-%']
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(2, (int) $rows[1]['id']);
        $this->assertEquals(5, (int) $rows[2]['id']);

        foreach ($rows as $row) {
            $this->assertStringStartsWith('SKU-WIDGET-', $row['sku']);
        }
    }

    public function testPreparedUpdateArithmeticSelfRef(): void
    {
        $stmt = $this->pdo->prepare('UPDATE mp_wbin_bins SET current_qty = current_qty + ? WHERE id = ?');
        $stmt->execute([25, 2]);

        $rows = $this->ztdQuery("SELECT current_qty FROM mp_wbin_bins WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertSame(55, (int) $rows[0]['current_qty']);
    }

    public function testMixedExecAndPrepareConsistency(): void
    {
        // Step 1: exec INSERT
        $this->ztdExec("INSERT INTO mp_wbin_items VALUES (8, 2, 'SKU-MIXED-001', 'Mixed test item', 5)");

        // Step 2: prepare+execute SELECT
        $rows = $this->ztdPrepareAndExecute(
            "SELECT * FROM mp_wbin_items WHERE id = ?",
            [8]
        );
        $this->assertCount(1, $rows);
        $this->assertSame('SKU-MIXED-001', $rows[0]['sku']);

        // Step 3: exec UPDATE
        $this->ztdExec("UPDATE mp_wbin_items SET qty = 10 WHERE id = 8");

        // Step 4: prepare+execute SELECT again
        $rows = $this->ztdPrepareAndExecute(
            "SELECT * FROM mp_wbin_items WHERE id = ?",
            [8]
        );
        $this->assertCount(1, $rows);
        $this->assertSame(10, (int) $rows[0]['qty']);

        // Step 5: exec DELETE
        $this->ztdExec("DELETE FROM mp_wbin_items WHERE id = 8");

        // Step 6: prepare+execute SELECT again
        $rows = $this->ztdPrepareAndExecute(
            "SELECT * FROM mp_wbin_items WHERE id = ?",
            [8]
        );
        $this->assertCount(0, $rows);
    }

    public function testInsertSelectFromJoinedTables(): void
    {
        $this->ztdExec(
            "INSERT INTO mp_wbin_transfers (id, item_id, from_bin, to_bin, qty, status)
             SELECT i.id + 100, i.id, i.bin_id, 4, i.qty, 'planned'
             FROM mp_wbin_items i
             JOIN mp_wbin_bins b ON i.bin_id = b.id
             WHERE b.location LIKE 'A-%'"
        );

        $rows = $this->ztdQuery(
            "SELECT id, item_id, from_bin, to_bin, qty, status
             FROM mp_wbin_transfers
             WHERE status = 'planned'
             ORDER BY id"
        );

        $this->assertCount(4, $rows);

        foreach ($rows as $row) {
            $this->assertSame('planned', $row['status']);
            $this->assertEquals(4, (int) $row['to_bin']);
        }
    }

    public function testUpdateDecrementAndVerifyFloor(): void
    {
        $this->ztdExec("UPDATE mp_wbin_bins SET current_qty = current_qty - 40 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT current_qty FROM mp_wbin_bins WHERE id = 1");
        $this->assertSame(5, (int) $rows[0]['current_qty']);

        $this->ztdExec("UPDATE mp_wbin_bins SET current_qty = current_qty - 10 WHERE id = 4");

        $rows = $this->ztdQuery("SELECT current_qty FROM mp_wbin_bins WHERE id = 4");
        $this->assertSame(0, (int) $rows[0]['current_qty']);
    }

    public function testSelectAfterDeleteAndInsert(): void
    {
        $this->ztdExec("DELETE FROM mp_wbin_items WHERE bin_id = 1");

        $this->ztdExec("INSERT INTO mp_wbin_items VALUES (10, 1, 'SKU-NEW-001', 'Replacement item', 50)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_wbin_items WHERE bin_id = 1");
        $this->assertSame(1, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT sku FROM mp_wbin_items WHERE bin_id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('SKU-NEW-001', $rows[0]['sku']);
    }

    public function testJoinWithLikeInWhereAfterShadowOps(): void
    {
        $this->ztdExec("INSERT INTO mp_wbin_items VALUES (11, 4, 'SKU-WIDGET-003', 'Deluxe Widget', 25)");

        $rows = $this->ztdQuery(
            "SELECT i.sku, b.location
             FROM mp_wbin_items i
             JOIN mp_wbin_bins b ON i.bin_id = b.id
             WHERE i.sku LIKE 'SKU-WIDGET-%'
             ORDER BY i.id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('SKU-WIDGET-001', $rows[0]['sku']);
        $this->assertSame('A-01', $rows[0]['location']);
        $this->assertSame('SKU-WIDGET-002', $rows[1]['sku']);
        $this->assertSame('A-01', $rows[1]['location']);
        $this->assertSame('SKU-WIDGET-001', $rows[2]['sku']);
        $this->assertSame('B-01', $rows[2]['location']);
        $this->assertSame('SKU-WIDGET-003', $rows[3]['sku']);
        $this->assertSame('B-02', $rows[3]['location']);
    }
}
