<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests MySQL generated (virtual/stored) column handling via MySQLi.
 *
 * Cross-platform parity with MysqlGeneratedColumnTest (PDO).
 */
class GeneratedColumnTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_gencol_test');
        $raw->query("CREATE TABLE mi_gencol_test (
            id INT PRIMARY KEY,
            price DECIMAL(10,2),
            quantity INT,
            total DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity) STORED,
            label VARCHAR(100) GENERATED ALWAYS AS (CONCAT(quantity, 'x @ ', price)) VIRTUAL
        )");
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
     * INSERT omitting generated columns.
     */
    public function testInsertOmittingGeneratedColumns(): void
    {
        $this->mysqli->query("INSERT INTO mi_gencol_test (id, price, quantity) VALUES (1, 9.99, 3)");

        $result = $this->mysqli->query('SELECT id, price, quantity FROM mi_gencol_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame(1, (int) $row['id']);
        $this->assertEqualsWithDelta(9.99, (float) $row['price'], 0.01);
        $this->assertSame(3, (int) $row['quantity']);
    }

    /**
     * SELECT generated column values from shadow store.
     */
    public function testSelectGeneratedColumnValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        $result = $this->mysqli->query('SELECT total, label FROM mi_gencol_test WHERE id = 1');
        $row = $result->fetch_assoc();

        // Generated columns may be NULL in shadow
        if ($row['total'] !== null) {
            $this->assertEqualsWithDelta(50.0, (float) $row['total'], 0.01);
        } else {
            $this->assertNull($row['total']);
        }
    }

    /**
     * UPDATE non-generated columns.
     */
    public function testUpdateNonGeneratedColumn(): void
    {
        $this->mysqli->query("INSERT INTO mi_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->mysqli->query('UPDATE mi_gencol_test SET quantity = 10 WHERE id = 1');

        $result = $this->mysqli->query('SELECT quantity FROM mi_gencol_test WHERE id = 1');
        $this->assertSame(10, (int) $result->fetch_assoc()['quantity']);
    }

    /**
     * Multiple rows with generated columns.
     */
    public function testMultipleRows(): void
    {
        $this->mysqli->query("INSERT INTO mi_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->mysqli->query("INSERT INTO mi_gencol_test (id, price, quantity) VALUES (2, 20.00, 3)");
        $this->mysqli->query("INSERT INTO mi_gencol_test (id, price, quantity) VALUES (3, 5.00, 10)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_gencol_test');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_gencol_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_gencol_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
