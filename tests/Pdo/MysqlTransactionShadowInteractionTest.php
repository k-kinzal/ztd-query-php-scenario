<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that the ZTD shadow store is independent of physical transaction state (MySQL PDO).
 * Shadow data persists after rollback; commit does not flush shadow to physical.
 * @spec SPEC-4.8
 */
class MysqlTransactionShadowInteractionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_txs_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_txs_items'];
    }


    public function testShadowDataPersistsAfterRollback(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO mysql_txs_items VALUES (1, 'Widget', 29.99)");
        $this->pdo->exec("INSERT INTO mysql_txs_items VALUES (2, 'Gadget', 49.99)");
        $this->pdo->rollBack();

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_txs_items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testShadowUpdatePersistsAfterRollback(): void
    {
        $this->pdo->exec("INSERT INTO mysql_txs_items VALUES (1, 'Widget', 29.99)");

        $this->pdo->beginTransaction();
        $this->pdo->exec("UPDATE mysql_txs_items SET price = 99.99 WHERE id = 1");
        $this->pdo->rollBack();

        $stmt = $this->pdo->query("SELECT price FROM mysql_txs_items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.01);
    }

    public function testCommitDoesNotFlushShadowToPhysical(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO mysql_txs_items VALUES (1, 'Widget', 29.99)");
        $this->pdo->commit();

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_txs_items");
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_txs_items");
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testMultipleTransactionCyclesAccumulateShadow(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO mysql_txs_items VALUES (1, 'Item 1', 10.00)");
        $this->pdo->commit();

        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO mysql_txs_items VALUES (2, 'Item 2', 20.00)");
        $this->pdo->rollBack();

        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO mysql_txs_items VALUES (3, 'Item 3', 30.00)");
        $this->pdo->commit();

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_txs_items");
        $this->assertSame(3, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testPreparedStatementInRolledBackTransaction(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->prepare("INSERT INTO mysql_txs_items VALUES (?, ?, ?)");
        $stmt->execute([1, 'Prepared', 15.00]);
        $stmt->execute([2, 'Also Prepared', 25.00]);

        $this->pdo->rollBack();

        $stmt = $this->pdo->query("SELECT * FROM mysql_txs_items ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Prepared', $rows[0]['name']);
        $this->assertSame('Also Prepared', $rows[1]['name']);
    }
}
