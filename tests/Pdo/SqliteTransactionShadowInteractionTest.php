<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests the interaction between database transactions and the ZTD shadow store.
 * Key behavior: the shadow store is independent of physical transaction state.
 * Shadow data persists after rollback, and commit does not flush shadow to physical.
 */
class SqliteTransactionShadowInteractionTest extends TestCase
{
    private ZtdPdo $pdo;
    private PDO $raw;

    protected function setUp(): void
    {
        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE txs_items (id INTEGER PRIMARY KEY, name TEXT, price REAL)');

        $this->pdo = ZtdPdo::fromPdo($this->raw);
    }

    /**
     * Shadow data inserted during a transaction persists after rollback.
     * The shadow store is NOT rolled back with the physical transaction.
     */
    public function testShadowDataPersistsAfterRollback(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO txs_items VALUES (1, 'Widget', 29.99)");
        $this->pdo->exec("INSERT INTO txs_items VALUES (2, 'Gadget', 49.99)");
        $this->pdo->rollBack();

        // Shadow data is still visible after rollback
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    /**
     * Shadow UPDATE persists after rollback.
     */
    public function testShadowUpdatePersistsAfterRollback(): void
    {
        $this->pdo->exec("INSERT INTO txs_items VALUES (1, 'Widget', 29.99)");

        $this->pdo->beginTransaction();
        $this->pdo->exec("UPDATE txs_items SET price = 99.99 WHERE id = 1");
        $this->pdo->rollBack();

        // Update persists in shadow
        $stmt = $this->pdo->query("SELECT price FROM txs_items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.01);
    }

    /**
     * Shadow DELETE persists after rollback.
     */
    public function testShadowDeletePersistsAfterRollback(): void
    {
        $this->pdo->exec("INSERT INTO txs_items VALUES (1, 'Widget', 29.99)");
        $this->pdo->exec("INSERT INTO txs_items VALUES (2, 'Gadget', 49.99)");

        $this->pdo->beginTransaction();
        $this->pdo->exec("DELETE FROM txs_items WHERE id = 1");
        $this->pdo->rollBack();

        // Delete persists in shadow
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);
    }

    /**
     * Commit does NOT flush shadow data to the physical database.
     */
    public function testCommitDoesNotFlushShadowToPhysical(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO txs_items VALUES (1, 'Widget', 29.99)");
        $this->pdo->commit();

        // Shadow data visible
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);

        // Physical table still empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }

    /**
     * Multiple transaction cycles accumulate shadow data.
     */
    public function testMultipleTransactionCyclesAccumulateShadow(): void
    {
        // Transaction 1: insert + commit
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO txs_items VALUES (1, 'Item 1', 10.00)");
        $this->pdo->commit();

        // Transaction 2: insert + rollback (shadow persists)
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO txs_items VALUES (2, 'Item 2', 20.00)");
        $this->pdo->rollBack();

        // Transaction 3: insert + commit
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO txs_items VALUES (3, 'Item 3', 30.00)");
        $this->pdo->commit();

        // All 3 items in shadow (including the rolled-back one)
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    /**
     * InTransaction reflects physical state, not shadow state.
     */
    public function testInTransactionReflectsPhysicalState(): void
    {
        $this->assertFalse($this->pdo->inTransaction());

        $this->pdo->beginTransaction();
        $this->assertTrue($this->pdo->inTransaction());

        $this->pdo->exec("INSERT INTO txs_items VALUES (1, 'Widget', 29.99)");
        $this->assertTrue($this->pdo->inTransaction());

        $this->pdo->rollBack();
        $this->assertFalse($this->pdo->inTransaction());
    }

    /**
     * Mixed shadow and physical operations during transaction.
     * Disabling ZTD mid-transaction writes to physical; rollback rolls back physical.
     */
    public function testMixedShadowAndPhysicalInTransaction(): void
    {
        $this->pdo->beginTransaction();

        // Shadow insert
        $this->pdo->exec("INSERT INTO txs_items VALUES (1, 'Shadow', 10.00)");

        // Physical insert (ZTD disabled)
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO txs_items VALUES (2, 'Physical', 20.00)");
        $this->pdo->enableZtd();

        $this->pdo->rollBack();

        // Shadow data persists
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);

        // Physical data rolled back
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }

    /**
     * Prepared statement within a transaction that's rolled back.
     * Shadow data from prepared statement persists.
     */
    public function testPreparedStatementInRolledBackTransaction(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->prepare("INSERT INTO txs_items VALUES (?, ?, ?)");
        $stmt->execute([1, 'Prepared', 15.00]);
        $stmt->execute([2, 'Also Prepared', 25.00]);

        $this->pdo->rollBack();

        // Shadow data from prepared statements persists
        $stmt = $this->pdo->query("SELECT * FROM txs_items ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Prepared', $rows[0]['name']);
        $this->assertSame('Also Prepared', $rows[1]['name']);
    }
}
