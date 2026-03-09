<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store consistency after complex sequences of operations.
 *
 * Exercises scenarios where the shadow store must maintain correct state
 * across multiple INSERT/UPDATE/DELETE operations, including re-inserting
 * deleted PKs and updating the same row multiple times.
 * @spec SPEC-4.1
 */
class SqliteShadowStoreConsistencyTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ssc_items (id INT PRIMARY KEY, name VARCHAR(50), qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ssc_items'];
    }

    /**
     * Delete then re-insert same PK: shadow should show new data.
     */
    public function testDeleteThenReinsertSamePk(): void
    {
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'original', 10)");
        $this->pdo->exec('DELETE FROM ssc_items WHERE id = 1');
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'replaced', 20)");

        $rows = $this->ztdQuery('SELECT * FROM ssc_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('replaced', $rows[0]['name']);
        $this->assertSame('20', (string) $rows[0]['qty']);
    }

    /**
     * Multiple updates to the same row: final state should reflect last update.
     */
    public function testMultipleUpdatesToSameRow(): void
    {
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'v1', 1)");
        $this->pdo->exec("UPDATE ssc_items SET name = 'v2', qty = 2 WHERE id = 1");
        $this->pdo->exec("UPDATE ssc_items SET name = 'v3', qty = 3 WHERE id = 1");
        $this->pdo->exec("UPDATE ssc_items SET name = 'v4', qty = 4 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT * FROM ssc_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('v4', $rows[0]['name']);
        $this->assertSame('4', (string) $rows[0]['qty']);
    }

    /**
     * Insert multiple rows, delete some, update remaining, insert more.
     */
    public function testMixedOperationsSequence(): void
    {
        // Insert batch
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'a', 10)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (2, 'b', 20)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (3, 'c', 30)");

        // Delete middle
        $this->pdo->exec('DELETE FROM ssc_items WHERE id = 2');

        // Update remaining
        $this->pdo->exec('UPDATE ssc_items SET qty = qty + 100 WHERE id IN (1, 3)');

        // Insert new rows
        $this->pdo->exec("INSERT INTO ssc_items VALUES (4, 'd', 40)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (5, 'e', 50)");

        $rows = $this->ztdQuery('SELECT * FROM ssc_items ORDER BY id');
        $this->assertCount(4, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
        $this->assertSame('110', (string) $rows[0]['qty']);
        $this->assertSame('3', (string) $rows[1]['id']);
        $this->assertSame('130', (string) $rows[1]['qty']);
        $this->assertSame('4', (string) $rows[2]['id']);
        $this->assertSame('40', (string) $rows[2]['qty']);
        $this->assertSame('5', (string) $rows[3]['id']);
        $this->assertSame('50', (string) $rows[3]['qty']);
    }

    /**
     * Delete all rows then verify empty.
     */
    public function testDeleteAllRows(): void
    {
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'a', 10)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (2, 'b', 20)");

        $this->pdo->exec('DELETE FROM ssc_items WHERE id = 1');
        $this->pdo->exec('DELETE FROM ssc_items WHERE id = 2');

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ssc_items');
        $this->assertSame('0', (string) $rows[0]['cnt']);
    }

    /**
     * Aggregate queries on shadow data after mixed operations.
     */
    public function testAggregateAfterMixedOperations(): void
    {
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'x', 100)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (2, 'y', 200)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (3, 'z', 300)");
        $this->pdo->exec('UPDATE ssc_items SET qty = 150 WHERE id = 1');
        $this->pdo->exec('DELETE FROM ssc_items WHERE id = 2');

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(qty) AS total, AVG(qty) AS avg_qty FROM ssc_items');
        $this->assertSame('2', (string) $rows[0]['cnt']);
        $this->assertSame('450', (string) $rows[0]['total']); // 150 + 300
        $this->assertEquals(225.0, (float) $rows[0]['avg_qty']);
    }

    /**
     * UPDATE with self-referencing arithmetic (qty = qty * 2) applied multiple times.
     */
    public function testSelfReferencingUpdateMultipleTimes(): void
    {
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'doubler', 1)");

        for ($i = 0; $i < 5; $i++) {
            $this->pdo->exec('UPDATE ssc_items SET qty = qty * 2 WHERE id = 1');
        }

        $rows = $this->ztdQuery('SELECT qty FROM ssc_items WHERE id = 1');
        // 1 * 2^5 = 32
        $this->assertSame('32', (string) $rows[0]['qty']);
    }

    /**
     * INSERT then immediately SELECT with WHERE on the inserted value.
     */
    public function testInsertThenSelectWithCondition(): void
    {
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'needle', 999)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (2, 'hay', 1)");
        $this->pdo->exec("INSERT INTO ssc_items VALUES (3, 'hay', 2)");

        $rows = $this->ztdQuery("SELECT * FROM ssc_items WHERE name = 'needle'");
        $this->assertCount(1, $rows);
        $this->assertSame('999', (string) $rows[0]['qty']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO ssc_items VALUES (1, 'test', 1)");
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ssc_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
