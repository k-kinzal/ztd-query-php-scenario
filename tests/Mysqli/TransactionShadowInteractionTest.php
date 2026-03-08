<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that the ZTD shadow store is independent of physical transaction state (MySQLi).
 * Shadow data persists after rollback; commit does not flush shadow to physical.
 * @spec SPEC-4.8
 */
class TransactionShadowInteractionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_txs_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mi_txs_items'];
    }


    public function testShadowDataPersistsAfterRollback(): void
    {
        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txs_items VALUES (1, 'Widget', 29.99)");
        $this->mysqli->query("INSERT INTO mi_txs_items VALUES (2, 'Gadget', 49.99)");
        $this->mysqli->rollback();

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_txs_items");
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testShadowUpdatePersistsAfterRollback(): void
    {
        $this->mysqli->query("INSERT INTO mi_txs_items VALUES (1, 'Widget', 29.99)");

        $this->mysqli->begin_transaction();
        $this->mysqli->query("UPDATE mi_txs_items SET price = 99.99 WHERE id = 1");
        $this->mysqli->rollback();

        $result = $this->mysqli->query("SELECT price FROM mi_txs_items WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.01);
    }

    public function testCommitDoesNotFlushShadowToPhysical(): void
    {
        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txs_items VALUES (1, 'Widget', 29.99)");
        $this->mysqli->commit();

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_txs_items");
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_txs_items");
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    public function testMultipleTransactionCyclesAccumulateShadow(): void
    {
        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txs_items VALUES (1, 'Item 1', 10.00)");
        $this->mysqli->commit();

        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txs_items VALUES (2, 'Item 2', 20.00)");
        $this->mysqli->rollback();

        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txs_items VALUES (3, 'Item 3', 30.00)");
        $this->mysqli->commit();

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_txs_items");
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }
}
