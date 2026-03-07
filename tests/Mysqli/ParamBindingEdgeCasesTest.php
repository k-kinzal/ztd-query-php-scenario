<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests prepared statement parameter binding edge cases on MySQLi,
 * matching PDO parity tests: positional params, by-reference rebinding,
 * CTE data snapshotting, and re-execution with different params.
 */
class ParamBindingEdgeCasesTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_pb_items');
        $raw->query('CREATE TABLE mi_pb_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), active TINYINT)');
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

        $this->mysqli->query("INSERT INTO mi_pb_items (id, name, price, active) VALUES (1, 'Widget', 10.50, 1)");
        $this->mysqli->query("INSERT INTO mi_pb_items (id, name, price, active) VALUES (2, 'Gadget', 25.00, 0)");
        $this->mysqli->query("INSERT INTO mi_pb_items (id, name, price, active) VALUES (3, 'Doohickey', 5.75, 1)");
    }

    public function testPositionalParams(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pb_items WHERE price > ? AND active = ?');
        $price = 8.0;
        $active = 1;
        $stmt->bind_param('di', $price, $active);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }

    public function testBindParamByReference(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pb_items WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame('Widget', $result->fetch_assoc()['name']);

        $id = 3;
        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame('Doohickey', $result->fetch_assoc()['name']);
    }

    public function testPreparedSelectBeforeInsertReturnsEmpty(): void
    {
        $selectStmt = $this->mysqli->prepare('SELECT name FROM mi_pb_items WHERE id = ?');
        $selectId = 20;
        $selectStmt->bind_param('i', $selectId);

        $this->mysqli->query("INSERT INTO mi_pb_items (id, name, price, active) VALUES (20, 'Late', 1.0, 1)");

        // CTE snapshot from prepare time → no id=20
        $selectStmt->execute();
        $result = $selectStmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertNull($row);
    }

    public function testReExecuteWithDifferentParams(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pb_items WHERE active = ?');
        $active = 1;
        $stmt->bind_param('i', $active);

        $stmt->execute();
        $this->assertCount(2, $stmt->get_result()->fetch_all(MYSQLI_ASSOC));

        $active = 0;
        $stmt->execute();
        $inactive = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $inactive);
        $this->assertSame('Gadget', $inactive[0]['name']);
    }

    public function testExecuteQueryPositionalParams(): void
    {
        if (!method_exists($this->mysqli, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }

        $result = $this->mysqli->execute_query(
            'SELECT name FROM mi_pb_items WHERE price > ? AND active = ?',
            [8.0, 1]
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
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
        $raw->query('DROP TABLE IF EXISTS mi_pb_items');
        $raw->close();
    }
}
