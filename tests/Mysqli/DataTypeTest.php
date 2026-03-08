<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that the shadow store correctly handles various SQL data types on MySQL via MySQLi,
 * including DATE, DATETIME, DECIMAL, BOOLEAN, and BIGINT.
 * @spec SPEC-3.4
 */
class DataTypeTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysqli_dtype_test (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            price DECIMAL(10,2),
            created_date DATE,
            created_at DATETIME,
            is_active TINYINT(1),
            quantity BIGINT
        )';
    }

    protected function getTableNames(): array
    {
        return ['mysqli_dtype_test'];
    }


    public function testDateValue(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mysqli_dtype_test (id, name, created_date) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'item';
        $date = '2024-06-15';
        $stmt->bind_param('iss', $id, $name, $date);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT created_date FROM mysqli_dtype_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('2024-06-15', $row['created_date']);
    }

    public function testDatetimeValue(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mysqli_dtype_test (id, name, created_at) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'item';
        $dt = '2024-06-15 14:30:00';
        $stmt->bind_param('iss', $id, $name, $dt);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT created_at FROM mysqli_dtype_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('2024-06-15 14:30:00', $row['created_at']);
    }

    public function testDecimalPrecision(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mysqli_dtype_test (id, name, price) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'item';
        $price = '99.99';
        $stmt->bind_param('isd', $id, $name, $price);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT price FROM mysqli_dtype_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('99.99', $row['price']);
    }

    public function testBooleanAsTinyint(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, is_active) VALUES (1, 'active', 1)");
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, is_active) VALUES (2, 'inactive', 0)");

        $result = $this->mysqli->query('SELECT * FROM mysqli_dtype_test WHERE is_active = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('active', $rows[0]['name']);
    }

    public function testBigintValue(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mysqli_dtype_test (id, name, quantity) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'big';
        $qty = 9223372036854775807; // PHP_INT_MAX
        $stmt->bind_param('isi', $id, $name, $qty);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT quantity FROM mysqli_dtype_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertEquals(9223372036854775807, $row['quantity']);
    }

    public function testDateComparison(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, created_date) VALUES (1, 'old', '2023-01-01')");
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, created_date) VALUES (2, 'new', '2024-06-15')");

        $result = $this->mysqli->query("SELECT * FROM mysqli_dtype_test WHERE created_date > '2024-01-01' ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('new', $rows[0]['name']);
    }

    public function testDecimalAggregation(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, price) VALUES (1, 'a', 10.50)");
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, price) VALUES (2, 'b', 20.75)");
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, price) VALUES (3, 'c', 5.25)");

        $result = $this->mysqli->query('SELECT SUM(price) as total FROM mysqli_dtype_test');
        $row = $result->fetch_assoc();
        $this->assertSame('36.50', $row['total']);
    }

    public function testDataTypeIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_dtype_test (id, name, price, created_date, is_active) VALUES (1, 'full', 99.99, '2024-06-15', 1)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM mysqli_dtype_test');
        $this->assertSame(0, $result->num_rows);
    }
}
