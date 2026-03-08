<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that the shadow store correctly handles various SQL data types,
 * including dates, timestamps, decimals, booleans, and large integers.
 * @spec SPEC-3.4
 */
class SqliteDataTypeTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE dtype_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL,
            created_at TEXT,
            is_active INTEGER,
            quantity INTEGER
        )';
    }

    protected function getTableNames(): array
    {
        return ['dtype_test'];
    }


    public function testDateStringValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, created_at) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '2024-06-15']);

        $stmt = $this->pdo->query('SELECT created_at FROM dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-06-15', $row['created_at']);
    }

    public function testTimestampStringValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, created_at) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '2024-06-15 14:30:00']);

        $stmt = $this->pdo->query('SELECT created_at FROM dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-06-15 14:30:00', $row['created_at']);
    }

    public function testDecimalPrecision(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, price) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', 99.99]);

        $stmt = $this->pdo->query('SELECT price FROM dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.001);
    }

    public function testSmallDecimalPrecision(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, price) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', 0.01]);

        $stmt = $this->pdo->query('SELECT price FROM dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(0.01, (float) $row['price'], 0.001);
    }

    public function testBooleanAsInteger(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, is_active) VALUES (?, ?, ?)');
        $stmt->execute([1, 'active', 1]);
        $stmt->execute([2, 'inactive', 0]);

        $stmt = $this->pdo->query('SELECT * FROM dtype_test WHERE is_active = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('active', $rows[0]['name']);
    }

    public function testLargeInteger(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, quantity) VALUES (?, ?, ?)');
        $stmt->execute([1, 'big', 2147483647]); // INT_MAX

        $stmt = $this->pdo->query('SELECT quantity FROM dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2147483647, (int) $row['quantity']);
    }

    public function testNegativeValues(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, price, quantity) VALUES (?, ?, ?, ?)');
        $stmt->execute([1, 'refund', -50.00, -10]);

        $stmt = $this->pdo->query('SELECT price, quantity FROM dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(-50.00, (float) $row['price'], 0.001);
        $this->assertSame(-10, (int) $row['quantity']);
    }

    public function testZeroValues(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, price, quantity) VALUES (?, ?, ?, ?)');
        $stmt->execute([1, 'free', 0.00, 0]);

        $stmt = $this->pdo->query('SELECT price, quantity FROM dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(0.00, (float) $row['price'], 0.001);
        $this->assertSame(0, (int) $row['quantity']);
    }

    public function testDateComparison(): void
    {
        $this->pdo->exec("INSERT INTO dtype_test (id, name, created_at) VALUES (1, 'old', '2023-01-01')");
        $this->pdo->exec("INSERT INTO dtype_test (id, name, created_at) VALUES (2, 'new', '2024-06-15')");

        $stmt = $this->pdo->query("SELECT * FROM dtype_test WHERE created_at > '2024-01-01' ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('new', $rows[0]['name']);
    }

    public function testDecimalAggregation(): void
    {
        $this->pdo->exec("INSERT INTO dtype_test (id, name, price) VALUES (1, 'a', 10.50)");
        $this->pdo->exec("INSERT INTO dtype_test (id, name, price) VALUES (2, 'b', 20.75)");
        $this->pdo->exec("INSERT INTO dtype_test (id, name, price) VALUES (3, 'c', 5.25)");

        $stmt = $this->pdo->query('SELECT SUM(price) as total, AVG(price) as avg_price FROM dtype_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(36.50, (float) $row['total'], 0.01);
        $this->assertEqualsWithDelta(12.17, (float) $row['avg_price'], 0.01);
    }

    public function testMixedNullAndNonNull(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO dtype_test (id, name, price, created_at) VALUES (?, ?, ?, ?)');
        $stmt->execute([1, 'with_all', 10.00, '2024-01-01']);
        $stmt->execute([2, 'no_price', null, '2024-01-02']);
        $stmt->execute([3, 'no_date', 20.00, null]);

        $stmt = $this->pdo->query('SELECT * FROM dtype_test WHERE price IS NOT NULL ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        $stmt = $this->pdo->query('SELECT * FROM dtype_test WHERE created_at IS NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('no_date', $rows[0]['name']);
    }
}
