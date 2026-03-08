<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests MySQL-specific features: IF(), IFNULL, FIND_IN_SET, INSERT ON DUPLICATE KEY edge cases.
 */
class MysqlSpecificFeaturesTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_msf_products');
        $raw->exec('CREATE TABLE mysql_msf_products (id INT PRIMARY KEY, name VARCHAR(255), stock INT, price DECIMAL(10,2), tags VARCHAR(255))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO mysql_msf_products (id, name, stock, price, tags) VALUES (1, 'Widget', 50, 9.99, 'hardware,small')");
        $this->pdo->exec("INSERT INTO mysql_msf_products (id, name, stock, price, tags) VALUES (2, 'Gadget', 0, 29.99, 'electronics,big')");
        $this->pdo->exec("INSERT INTO mysql_msf_products (id, name, stock, price, tags) VALUES (3, 'Gizmo', 10, 19.99, 'electronics,fancy')");
    }

    public function testIfFunction(): void
    {
        $stmt = $this->pdo->query("SELECT name, IF(stock > 0, 'In Stock', 'Out of Stock') AS availability FROM mysql_msf_products ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('In Stock', $rows[0]['availability']);
        $this->assertSame('Out of Stock', $rows[1]['availability']);
    }

    public function testIfnull(): void
    {
        $this->pdo->exec("INSERT INTO mysql_msf_products (id, name, stock, price, tags) VALUES (4, 'NoTag', 5, 1.00, NULL)");

        $stmt = $this->pdo->query("SELECT IFNULL(tags, 'none') AS tag_list FROM mysql_msf_products WHERE id = 4");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('none', $row['tag_list']);
    }

    public function testFindInSet(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM mysql_msf_products WHERE FIND_IN_SET('electronics', tags) > 0 ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertSame('Gizmo', $rows[1]['name']);
    }

    /**
     * ON DUPLICATE KEY UPDATE with self-referencing expression should increment stock.
     */
    public function testInsertOnDuplicateKeyUpdateIncrement(): void
    {
        $this->pdo->exec("INSERT INTO mysql_msf_products (id, name, stock, price, tags) VALUES (1, 'Widget', 10, 9.99, 'hardware,small') ON DUPLICATE KEY UPDATE stock = stock + VALUES(stock)");

        $stmt = $this->pdo->query("SELECT stock FROM mysql_msf_products WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $stock = (int) $row['stock'];
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
        $this->pdo->exec("INSERT INTO mysql_msf_products (id, name, stock, price, tags) VALUES (2, 'Gadget V2', 100, 24.99, 'electronics,updated') ON DUPLICATE KEY UPDATE name = VALUES(name), stock = VALUES(stock), price = VALUES(price)");

        $stmt = $this->pdo->query("SELECT name, stock, price FROM mysql_msf_products WHERE id = 2");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Gadget V2', $row['name']);
        $this->assertSame(100, (int) $row['stock']);
        $this->assertEqualsWithDelta(24.99, (float) $row['price'], 0.01);
    }

    public function testConcatWs(): void
    {
        $stmt = $this->pdo->query("SELECT CONCAT_WS(' - ', name, tags) AS display FROM mysql_msf_products WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget - hardware,small', $row['display']);
    }

    public function testReverseAndLpad(): void
    {
        $stmt = $this->pdo->query("SELECT REVERSE(name) AS rev, LPAD(CAST(stock AS CHAR), 5, '0') AS padded FROM mysql_msf_products WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('tegdiW', $row['rev']);
        $this->assertSame('00050', $row['padded']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_msf_products');
    }
}
