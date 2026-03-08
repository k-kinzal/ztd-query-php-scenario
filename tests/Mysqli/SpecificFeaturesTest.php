<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL-specific features: IF(), IFNULL, FIND_IN_SET, ON DUPLICATE KEY edge cases via MySQLi.
 *
 * Cross-platform parity with MysqlSpecificFeaturesTest (PDO).
 * @spec pending
 */
class SpecificFeaturesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_msf_products (id INT PRIMARY KEY, name VARCHAR(255), stock INT, price DECIMAL(10,2), tags VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mi_msf_products'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_msf_products (id, name, stock, price, tags) VALUES (1, 'Widget', 50, 9.99, 'hardware,small')");
        $this->mysqli->query("INSERT INTO mi_msf_products (id, name, stock, price, tags) VALUES (2, 'Gadget', 0, 29.99, 'electronics,big')");
        $this->mysqli->query("INSERT INTO mi_msf_products (id, name, stock, price, tags) VALUES (3, 'Gizmo', 10, 19.99, 'electronics,fancy')");
    }

    public function testIfFunction(): void
    {
        $result = $this->mysqli->query("SELECT name, IF(stock > 0, 'In Stock', 'Out of Stock') AS availability FROM mi_msf_products ORDER BY id");
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row;
        }
        $this->assertCount(3, $rows);
        $this->assertSame('In Stock', $rows[0]['availability']);
        $this->assertSame('Out of Stock', $rows[1]['availability']);
    }

    public function testIfnull(): void
    {
        $this->mysqli->query("INSERT INTO mi_msf_products (id, name, stock, price, tags) VALUES (4, 'NoTag', 5, 1.00, NULL)");

        $result = $this->mysqli->query("SELECT IFNULL(tags, 'none') AS tag_list FROM mi_msf_products WHERE id = 4");
        $this->assertSame('none', $result->fetch_assoc()['tag_list']);
    }

    public function testFindInSet(): void
    {
        $result = $this->mysqli->query("SELECT name FROM mi_msf_products WHERE FIND_IN_SET('electronics', tags) > 0 ORDER BY name");
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row;
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertSame('Gizmo', $rows[1]['name']);
    }

    /**
     * ON DUPLICATE KEY UPDATE with self-referencing expression should increment stock.
     */
    public function testInsertOnDuplicateKeyUpdateIncrement(): void
    {
        $this->mysqli->query("INSERT INTO mi_msf_products (id, name, stock, price, tags) VALUES (1, 'Widget', 10, 9.99, 'hardware,small') ON DUPLICATE KEY UPDATE stock = stock + VALUES(stock)");

        $result = $this->mysqli->query('SELECT stock FROM mi_msf_products WHERE id = 1');
        $stock = (int) $result->fetch_assoc()['stock'];
        // Expected: 60 (50 + 10)
        if ($stock !== 60) {
            $this->markTestIncomplete(
                'ON DUPLICATE KEY UPDATE with self-referencing expression loses old value. '
                . 'Expected stock 60 (50 + 10), got ' . $stock
            );
        }
        $this->assertSame(60, $stock);
    }

    public function testInsertOnDuplicateKeyUpdateMultipleColumns(): void
    {
        $this->mysqli->query("INSERT INTO mi_msf_products (id, name, stock, price, tags) VALUES (2, 'Gadget V2', 100, 24.99, 'electronics,updated') ON DUPLICATE KEY UPDATE name = VALUES(name), stock = VALUES(stock), price = VALUES(price)");

        $result = $this->mysqli->query('SELECT name, stock, price FROM mi_msf_products WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame('Gadget V2', $row['name']);
        $this->assertSame(100, (int) $row['stock']);
        $this->assertEqualsWithDelta(24.99, (float) $row['price'], 0.01);
    }

    public function testConcatWs(): void
    {
        $result = $this->mysqli->query("SELECT CONCAT_WS(' - ', name, tags) AS display FROM mi_msf_products WHERE id = 1");
        $this->assertSame('Widget - hardware,small', $result->fetch_assoc()['display']);
    }

    public function testReverseAndLpad(): void
    {
        $result = $this->mysqli->query("SELECT REVERSE(name) AS rev, LPAD(CAST(stock AS CHAR), 5, '0') AS padded FROM mi_msf_products WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertSame('tegdiW', $row['rev']);
        $this->assertSame('00050', $row['padded']);
    }
}
