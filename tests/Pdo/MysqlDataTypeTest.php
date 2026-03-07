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
 * Tests that the shadow store correctly handles various SQL data types on MySQL,
 * including DATE, DATETIME, DECIMAL, BOOLEAN, and BIGINT.
 */
class MysqlDataTypeTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_dtype_test');
        $raw->exec('CREATE TABLE mysql_dtype_test (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            price DECIMAL(10,2),
            created_date DATE,
            created_at DATETIME,
            is_active BOOLEAN,
            quantity BIGINT
        )');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testDateValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_dtype_test (id, name, created_date) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '2024-06-15']);

        $stmt = $this->pdo->query('SELECT created_date FROM mysql_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-06-15', $row['created_date']);
    }

    public function testDatetimeValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_dtype_test (id, name, created_at) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '2024-06-15 14:30:00']);

        $stmt = $this->pdo->query('SELECT created_at FROM mysql_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-06-15 14:30:00', $row['created_at']);
    }

    public function testDecimalPrecision(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_dtype_test (id, name, price) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '99.99']);

        $stmt = $this->pdo->query('SELECT price FROM mysql_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('99.99', $row['price']);
    }

    public function testBooleanValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_dtype_test (id, name, is_active) VALUES (?, ?, ?)');
        $stmt->execute([1, 'active', true]);
        $stmt->execute([2, 'inactive', false]);

        $stmt = $this->pdo->query('SELECT * FROM mysql_dtype_test WHERE is_active = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('active', $rows[0]['name']);
    }

    public function testBigintValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_dtype_test (id, name, quantity) VALUES (?, ?, ?)');
        $stmt->execute([1, 'big', 9223372036854775807]); // PHP_INT_MAX

        $stmt = $this->pdo->query('SELECT quantity FROM mysql_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // BIGINT values may be returned as int or string depending on driver
        $this->assertEquals(9223372036854775807, $row['quantity']);
    }

    public function testNegativeDecimal(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_dtype_test (id, name, price) VALUES (?, ?, ?)');
        $stmt->execute([1, 'refund', '-50.00']);

        $stmt = $this->pdo->query('SELECT price FROM mysql_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('-50.00', $row['price']);
    }

    public function testDateComparison(): void
    {
        $this->pdo->exec("INSERT INTO mysql_dtype_test (id, name, created_date) VALUES (1, 'old', '2023-01-01')");
        $this->pdo->exec("INSERT INTO mysql_dtype_test (id, name, created_date) VALUES (2, 'new', '2024-06-15')");

        $stmt = $this->pdo->query("SELECT * FROM mysql_dtype_test WHERE created_date > '2024-01-01' ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('new', $rows[0]['name']);
    }

    public function testDecimalAggregation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_dtype_test (id, name, price) VALUES (1, 'a', 10.50)");
        $this->pdo->exec("INSERT INTO mysql_dtype_test (id, name, price) VALUES (2, 'b', 20.75)");
        $this->pdo->exec("INSERT INTO mysql_dtype_test (id, name, price) VALUES (3, 'c', 5.25)");

        $stmt = $this->pdo->query('SELECT SUM(price) as total FROM mysql_dtype_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('36.50', $row['total']);
    }

    public function testDataTypeIsolation(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_dtype_test (id, name, price, created_date, is_active, quantity) VALUES (?, ?, ?, ?, ?, ?)');
        $stmt->execute([1, 'full', '99.99', '2024-06-15', true, 100]);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_dtype_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_dtype_test');
    }
}
