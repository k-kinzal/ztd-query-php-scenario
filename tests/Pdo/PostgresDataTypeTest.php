<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests that the shadow store correctly handles various SQL data types on PostgreSQL,
 * including DATE, TIMESTAMP, NUMERIC, BOOLEAN, and BIGINT.
 * @spec pending
 */
class PostgresDataTypeTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_dtype_test (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            price NUMERIC(10,2),
            created_date DATE,
            created_at TIMESTAMP,
            is_active BOOLEAN,
            quantity BIGINT
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_dtype_test'];
    }


    public function testDateValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, created_date) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '2024-06-15']);

        $stmt = $this->pdo->query('SELECT created_date FROM pg_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-06-15', $row['created_date']);
    }

    public function testTimestampValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, created_at) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '2024-06-15 14:30:00']);

        $stmt = $this->pdo->query('SELECT created_at FROM pg_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-06-15 14:30:00', $row['created_at']);
    }

    public function testNumericPrecision(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, price) VALUES (?, ?, ?)');
        $stmt->execute([1, 'item', '99.99']);

        $stmt = $this->pdo->query('SELECT price FROM pg_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('99.99', $row['price']);
    }

    public function testBooleanTrueValue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, is_active) VALUES (?, ?, ?)');
        $stmt->execute([1, 'active', true]);

        $stmt = $this->pdo->query('SELECT * FROM pg_dtype_test WHERE is_active = true');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('active', $rows[0]['name']);
    }

    public function testBooleanFalseSelectThrows(): void
    {
        // KNOWN ISSUE: PostgreSQL CTE rewriter casts PHP false as CAST('' AS BOOLEAN),
        // which is invalid syntax for PostgreSQL boolean type.
        // INSERT succeeds, but the subsequent SELECT that builds the CTE fails.
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, is_active) VALUES (?, ?, ?)');
        $stmt->execute([1, 'active', true]);
        $stmt->execute([2, 'inactive', false]);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessageMatches('/invalid input syntax for type boolean/');

        // The CTE includes CAST('' AS BOOLEAN) for the false value, which PostgreSQL rejects
        $this->pdo->query('SELECT * FROM pg_dtype_test WHERE is_active = true');
    }

    public function testBigintOverflowOnSelect(): void
    {
        // KNOWN ISSUE: PostgreSQL CTE rewriter casts BIGINT values as CAST(... AS integer)
        // instead of CAST(... AS bigint), causing overflow for large values.
        // INSERT succeeds, but the CTE generation during SELECT fails.
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, quantity) VALUES (?, ?, ?)');
        $stmt->execute([1, 'big', 9223372036854775807]);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessageMatches('/out of range/');

        $this->pdo->query('SELECT quantity FROM pg_dtype_test WHERE id = 1');
    }

    public function testSmallBigintWorks(): void
    {
        // Small values fit in integer, so they work
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, quantity) VALUES (?, ?, ?)');
        $stmt->execute([1, 'small', 2147483647]); // INT_MAX

        $stmt = $this->pdo->query('SELECT quantity FROM pg_dtype_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(2147483647, $row['quantity']);
    }

    public function testDateComparison(): void
    {
        $this->pdo->exec("INSERT INTO pg_dtype_test (id, name, created_date) VALUES (1, 'old', '2023-01-01')");
        $this->pdo->exec("INSERT INTO pg_dtype_test (id, name, created_date) VALUES (2, 'new', '2024-06-15')");

        $stmt = $this->pdo->query("SELECT * FROM pg_dtype_test WHERE created_date > '2024-01-01' ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('new', $rows[0]['name']);
    }

    public function testNumericAggregation(): void
    {
        $this->pdo->exec("INSERT INTO pg_dtype_test (id, name, price) VALUES (1, 'a', 10.50)");
        $this->pdo->exec("INSERT INTO pg_dtype_test (id, name, price) VALUES (2, 'b', 20.75)");
        $this->pdo->exec("INSERT INTO pg_dtype_test (id, name, price) VALUES (3, 'c', 5.25)");

        $stmt = $this->pdo->query('SELECT SUM(price) as total FROM pg_dtype_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('36.50', $row['total']);
    }

    public function testDataTypeIsolation(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_dtype_test (id, name, price, created_date, is_active, quantity) VALUES (?, ?, ?, ?, ?, ?)');
        $stmt->execute([1, 'full', '99.99', '2024-06-15', true, 100]);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM pg_dtype_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
