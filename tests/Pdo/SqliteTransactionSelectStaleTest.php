<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SELECT consistency within transactions after shadow mutations.
 *
 * Users commonly wrap read-after-write patterns in transactions. This verifies
 * that SELECT within a transaction sees shadow data correctly, including
 * mutations made within the same transaction.
 *
 * @spec SPEC-2.1
 */
class SqliteTransactionSelectStaleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE tx_t (id INTEGER PRIMARY KEY, val TEXT, amount INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['tx_t'];
    }

    /**
     * Read-after-write in same transaction.
     */
    public function testReadAfterWriteInTransaction(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (1, 'in-tx', 100)");

        $rows = $this->ztdQuery('SELECT val, amount FROM tx_t WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('in-tx', $rows[0]['val']);
        $this->assertEquals(100, (int) $rows[0]['amount']);
        $this->pdo->commit();
    }

    /**
     * Multiple writes then read in same transaction.
     */
    public function testMultipleWritesThenReadInTransaction(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (1, 'a', 10)");
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (2, 'b', 20)");
        $this->pdo->exec("UPDATE tx_t SET amount = 30 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT id, amount FROM tx_t ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertEquals(30, (int) $rows[0]['amount']);
        $this->assertEquals(20, (int) $rows[1]['amount']);
        $this->pdo->commit();
    }

    /**
     * Rollback does NOT discard shadow data (by-design per SPEC-4.8).
     *
     * Shadow store is independent of physical transaction state.
     * rollBack() only affects the physical database, not the shadow store.
     */
    public function testRollbackDoesNotDiscardShadowData(): void
    {
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (1, 'before-tx', 100)");

        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (2, 'in-tx', 200)");
        $this->pdo->exec("UPDATE tx_t SET amount = 999 WHERE id = 1");
        $this->pdo->rollBack();

        // Shadow data persists after rollback (SPEC-4.8)
        $rows = $this->ztdQuery('SELECT id, val, amount FROM tx_t ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertEquals(999, (int) $rows[0]['amount']);
        $this->assertSame('in-tx', $rows[1]['val']);
    }

    /**
     * Prepared SELECT within transaction sees mutations.
     */
    public function testPreparedSelectInTransactionSeesMutations(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (1, 'a', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM tx_t WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('a', $row['val']);

        // Mutate within transaction
        $this->pdo->exec("UPDATE tx_t SET val = 'mutated' WHERE id = 1");

        // Fresh query should see mutation
        $rows = $this->ztdQuery('SELECT val FROM tx_t WHERE id = 1');
        $this->assertSame('mutated', $rows[0]['val']);

        $this->pdo->commit();
    }

    /**
     * Aggregate query within transaction after mutations.
     */
    public function testAggregateInTransactionAfterMutations(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
        $this->pdo->exec("DELETE FROM tx_t WHERE id = 2");

        $rows = $this->ztdQuery('SELECT SUM(amount) AS total, COUNT(*) AS cnt FROM tx_t');
        $this->assertEquals(40, (int) $rows[0]['total']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->pdo->commit();
    }

    /**
     * Nested operations: INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT.
     */
    public function testFullCrudCycleInTransaction(): void
    {
        $this->pdo->beginTransaction();

        // INSERT
        $this->pdo->exec("INSERT INTO tx_t (id, val, amount) VALUES (1, 'test', 100)");
        $rows = $this->ztdQuery('SELECT amount FROM tx_t WHERE id = 1');
        $this->assertEquals(100, (int) $rows[0]['amount']);

        // UPDATE
        $this->pdo->exec("UPDATE tx_t SET amount = 200 WHERE id = 1");
        $rows = $this->ztdQuery('SELECT amount FROM tx_t WHERE id = 1');
        $this->assertEquals(200, (int) $rows[0]['amount']);

        // DELETE
        $this->pdo->exec("DELETE FROM tx_t WHERE id = 1");
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM tx_t');
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        $this->pdo->commit();
    }
}
