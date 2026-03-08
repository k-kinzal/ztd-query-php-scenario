<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests INSERT...SELECT with aggregate functions through ZTD shadow store.
 *
 * Known limitation: Computed columns (COUNT, SUM, etc.) become NULL in shadow.
 * Direct column references transfer correctly.
 */
class SqliteInsertSelectAggregateTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE isa_orders (id INT PRIMARY KEY, category VARCHAR(20), amount DECIMAL(10,2))');
        $raw->exec('CREATE TABLE isa_summary (category VARCHAR(20), order_count INT, total_amount DECIMAL(10,2))');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO isa_orders VALUES (1, 'Electronics', 100.00)");
        $this->pdo->exec("INSERT INTO isa_orders VALUES (2, 'Electronics', 200.00)");
        $this->pdo->exec("INSERT INTO isa_orders VALUES (3, 'Books', 30.00)");
        $this->pdo->exec("INSERT INTO isa_orders VALUES (4, 'Books', 25.00)");
        $this->pdo->exec("INSERT INTO isa_orders VALUES (5, 'Electronics', 150.00)");
    }

    /**
     * INSERT...SELECT with GROUP BY — computed columns become NULL on SQLite.
     */
    public function testInsertSelectAggregateComputedColumnsNull(): void
    {
        $this->pdo->exec(
            'INSERT INTO isa_summary (category, order_count, total_amount)
             SELECT category, COUNT(*), SUM(amount) FROM isa_orders GROUP BY category'
        );

        $stmt = $this->pdo->query('SELECT * FROM isa_summary ORDER BY category');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Rows are inserted but aggregate columns are NULL (known SQLite limitation)
        $this->assertCount(2, $rows);
        $this->assertSame('Books', $rows[0]['category']);
        // COUNT(*) and SUM(amount) become NULL
        $this->assertNull($rows[0]['order_count']);
        $this->assertNull($rows[0]['total_amount']);
    }

    /**
     * INSERT...SELECT with direct column only (no aggregates) works.
     */
    public function testInsertSelectDirectColumnsWork(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE isa_src (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE isa_dst (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO isa_src VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO isa_src VALUES (2, 'Bob')");

        $pdo->exec('INSERT INTO isa_dst SELECT * FROM isa_src');

        $stmt = $pdo->query('SELECT COUNT(*) FROM isa_dst');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $pdo->query('SELECT name FROM isa_dst WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * INSERT...SELECT with WHERE filter works.
     */
    public function testInsertSelectWithFilter(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE isa_src2 (id INT PRIMARY KEY, name VARCHAR(50), active INT)');
        $raw->exec('CREATE TABLE isa_dst2 (id INT PRIMARY KEY, name VARCHAR(50), active INT)');
        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO isa_src2 VALUES (1, 'Alice', 1)");
        $pdo->exec("INSERT INTO isa_src2 VALUES (2, 'Bob', 0)");
        $pdo->exec("INSERT INTO isa_src2 VALUES (3, 'Charlie', 1)");

        $pdo->exec('INSERT INTO isa_dst2 SELECT * FROM isa_src2 WHERE active = 1');

        $stmt = $pdo->query('SELECT COUNT(*) FROM isa_dst2');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT...SELECT from same table (self-referencing) — computed values NULL.
     */
    public function testInsertSelectSameTableComputedNull(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE isa_self (id INT PRIMARY KEY, value INT)');
        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO isa_self VALUES (1, 100)");
        $pdo->exec("INSERT INTO isa_self VALUES (2, 200)");

        // Computed expression (id + 10, value * 2) — values may be NULL
        $pdo->exec('INSERT INTO isa_self (id, value) SELECT id + 10, value * 2 FROM isa_self');

        $stmt = $pdo->query('SELECT COUNT(*) FROM isa_self');
        $count = (int) $stmt->fetchColumn();
        // At least 2 more rows should be inserted
        $this->assertGreaterThanOrEqual(4, $count);

        // The computed id (id + 10) is NULL, so rows with non-NULL id won't find them
        // Query by IS NULL to find the inserted rows with NULL computed ids
        $stmt = $pdo->query('SELECT COUNT(*) FROM isa_self WHERE id IS NULL');
        $nullIdCount = (int) $stmt->fetchColumn();
        $this->assertGreaterThanOrEqual(2, $nullIdCount);
    }

    /**
     * INSERT...SELECT with ORDER BY and LIMIT.
     */
    public function testInsertSelectWithOrderByLimit(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE isa_ranked (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->exec('CREATE TABLE isa_top (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO isa_ranked VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO isa_ranked VALUES (2, 'Bob')");
        $pdo->exec("INSERT INTO isa_ranked VALUES (3, 'Charlie')");

        $pdo->exec('INSERT INTO isa_top SELECT * FROM isa_ranked ORDER BY name LIMIT 2');

        $stmt = $pdo->query('SELECT COUNT(*) FROM isa_top');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }
}
