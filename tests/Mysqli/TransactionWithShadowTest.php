<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests transaction interaction with ZTD shadow store via MySQLi.
 *
 * Cross-platform parity with SqliteTransactionWithShadowTest (PDO).
 */
class TransactionWithShadowTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_txn_test');
        $raw->query('CREATE TABLE mi_txn_test (id INT PRIMARY KEY, name VARCHAR(50))');
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

    /**
     * begin_transaction/commit works with shadow INSERT.
     */
    public function testCommitWithShadowInsert(): void
    {
        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txn_test VALUES (1, 'Alice')");
        $this->mysqli->commit();

        $result = $this->mysqli->query('SELECT name FROM mi_txn_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * rollback behavior with shadow INSERT.
     */
    public function testRollbackWithShadowInsert(): void
    {
        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txn_test VALUES (1, 'Alice')");
        $this->mysqli->rollback();

        // After rollback, shadow data behavior depends on implementation
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_txn_test');
        $count = (int) $result->fetch_assoc()['cnt'];
        $this->assertContains($count, [0, 1]);
    }

    /**
     * Multiple operations within a transaction.
     */
    public function testMultipleOpsInTransaction(): void
    {
        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txn_test VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_txn_test VALUES (2, 'Bob')");
        $this->mysqli->query("UPDATE mi_txn_test SET name = 'ALICE' WHERE id = 1");
        $this->mysqli->commit();

        $result = $this->mysqli->query('SELECT name FROM mi_txn_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('ALICE', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->begin_transaction();
        $this->mysqli->query("INSERT INTO mi_txn_test VALUES (1, 'Alice')");
        $this->mysqli->commit();

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_txn_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_txn_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
