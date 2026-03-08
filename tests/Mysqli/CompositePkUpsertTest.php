<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests ON DUPLICATE KEY UPDATE (UPSERT) with composite primary keys on MySQL MySQLi.
 *
 * MySQL uses INSERT ... ON DUPLICATE KEY UPDATE syntax.
 * Composite keys need correct matching across multiple columns.
 */
class CompositePkUpsertTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_cpk_upsert');
        $raw->query('CREATE TABLE mi_cpk_upsert (
            region_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL DEFAULT 0,
            price DECIMAL(10,2),
            PRIMARY KEY (region_id, product_id)
        )');
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

        $this->mysqli->query("INSERT INTO mi_cpk_upsert VALUES (1, 1, 10, 9.99)");
        $this->mysqli->query("INSERT INTO mi_cpk_upsert VALUES (1, 2, 20, 19.99)");
        $this->mysqli->query("INSERT INTO mi_cpk_upsert VALUES (2, 1, 15, 9.99)");
    }

    /**
     * ON DUPLICATE KEY UPDATE with composite PK — update existing.
     */
    public function testUpsertUpdateExisting(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_cpk_upsert (region_id, product_id, quantity, price)
             VALUES (1, 1, 50, 12.99)
             ON DUPLICATE KEY UPDATE quantity = VALUES(quantity), price = VALUES(price)"
        );

        $result = $this->mysqli->query('SELECT quantity, price FROM mi_cpk_upsert WHERE region_id = 1 AND product_id = 1');
        $row = $result->fetch_assoc();
        $this->assertEquals(50, (int) $row['quantity']);
        $this->assertEquals(12.99, (float) $row['price']);
    }

    /**
     * ON DUPLICATE KEY UPDATE — insert new row.
     */
    public function testUpsertInsertNew(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_cpk_upsert (region_id, product_id, quantity, price)
             VALUES (2, 2, 30, 24.99)
             ON DUPLICATE KEY UPDATE quantity = VALUES(quantity), price = VALUES(price)"
        );

        $result = $this->mysqli->query('SELECT quantity, price FROM mi_cpk_upsert WHERE region_id = 2 AND product_id = 2');
        $row = $result->fetch_assoc();
        $this->assertEquals(30, (int) $row['quantity']);
    }

    /**
     * Prepared upsert with composite PK.
     */
    public function testPreparedUpsert(): void
    {
        $stmt = $this->mysqli->prepare(
            'INSERT INTO mi_cpk_upsert (region_id, product_id, quantity, price)
             VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE quantity = VALUES(quantity), price = VALUES(price)'
        );
        $rid = 1;
        $pid = 1;
        $qty = 75;
        $price = 11.99;
        $stmt->bind_param('iiid', $rid, $pid, $qty, $price);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT quantity FROM mi_cpk_upsert WHERE region_id = 1 AND product_id = 1');
        $row = $result->fetch_assoc();
        $this->assertEquals(75, (int) $row['quantity']);
    }

    /**
     * Multiple upserts in sequence.
     */
    public function testMultipleUpserts(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_cpk_upsert (region_id, product_id, quantity)
             VALUES (1, 1, 100)
             ON DUPLICATE KEY UPDATE quantity = VALUES(quantity)"
        );
        $this->mysqli->query(
            "INSERT INTO mi_cpk_upsert (region_id, product_id, quantity)
             VALUES (1, 1, 200)
             ON DUPLICATE KEY UPDATE quantity = VALUES(quantity)"
        );

        $result = $this->mysqli->query('SELECT quantity FROM mi_cpk_upsert WHERE region_id = 1 AND product_id = 1');
        $row = $result->fetch_assoc();
        $this->assertEquals(200, (int) $row['quantity']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cpk_upsert');
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
            $raw->query('DROP TABLE IF EXISTS mi_cpk_upsert');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
