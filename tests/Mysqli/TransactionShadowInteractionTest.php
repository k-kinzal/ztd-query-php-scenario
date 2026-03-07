<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests that the ZTD shadow store is independent of physical transaction state (MySQLi).
 * Shadow data persists after rollback; commit does not flush shadow to physical.
 */
class TransactionShadowInteractionTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_txs_items');
        $raw->query('CREATE TABLE mi_txs_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
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

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_txs_items');
        $raw->close();
    }
}
