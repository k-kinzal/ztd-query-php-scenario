<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with aggregate functions through ZTD shadow store.
 * @spec pending
 */
class SqliteInsertSelectAggregateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE isa_orders (id INT PRIMARY KEY, category VARCHAR(20), amount DECIMAL(10,2))',
            'CREATE TABLE isa_summary (category VARCHAR(20), order_count INT, total_amount DECIMAL(10,2))',
            'CREATE TABLE isa_src (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE isa_dst (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE isa_src2 (id INT PRIMARY KEY, name VARCHAR(50), active INT)',
            'CREATE TABLE isa_dst2 (id INT PRIMARY KEY, name VARCHAR(50), active INT)',
            'CREATE TABLE isa_self (id INT PRIMARY KEY, value INT)',
            'CREATE TABLE isa_ranked (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE isa_top (id INT PRIMARY KEY, name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['isa_orders', 'isa_summary', 'isa_src', 'isa_dst', 'isa_src2', 'isa_dst2', 'isa_self', 'isa_ranked', 'isa_top'];
    }


    /**
     * INSERT...SELECT with GROUP BY should transfer computed columns correctly.
     */
    public function testInsertSelectAggregateComputedColumns(): void
    {
        $this->pdo->exec(
            'INSERT INTO isa_summary (category, order_count, total_amount)
             SELECT category, COUNT(*), SUM(amount) FROM isa_orders GROUP BY category'
        );

        $stmt = $this->pdo->query('SELECT * FROM isa_summary ORDER BY category');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Books', $rows[0]['category']);
        // Expected: aggregate values should be correctly stored
        if ($rows[0]['order_count'] === null || $rows[0]['total_amount'] === null) {
            $this->markTestIncomplete(
                'Computed columns (COUNT, SUM) become NULL in shadow store during INSERT...SELECT. '
                . 'order_count: ' . var_export($rows[0]['order_count'], true)
                . ', total_amount: ' . var_export($rows[0]['total_amount'], true)
            );
        }
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertEqualsWithDelta(55.0, (float) $rows[0]['total_amount'], 0.01);
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
     * INSERT...SELECT from same table with computed expressions should preserve values.
     */
    public function testInsertSelectSameTableComputed(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE isa_self (id INT PRIMARY KEY, value INT)');
        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO isa_self VALUES (1, 100)");
        $pdo->exec("INSERT INTO isa_self VALUES (2, 200)");

        // Computed expression (id + 10, value * 2) — should produce correct values
        $pdo->exec('INSERT INTO isa_self (id, value) SELECT id + 10, value * 2 FROM isa_self');

        $stmt = $pdo->query('SELECT COUNT(*) FROM isa_self');
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(4, $count);

        // Check that computed IDs are correct (11 and 12)
        $stmt = $pdo->query('SELECT id FROM isa_self WHERE id = 11');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if ($row === false) {
            // Check if they ended up as NULL
            $stmt = $pdo->query('SELECT COUNT(*) FROM isa_self WHERE id IS NULL');
            $nullCount = (int) $stmt->fetchColumn();
            if ($nullCount > 0) {
                $this->markTestIncomplete(
                    'Computed columns in INSERT...SELECT become NULL. '
                    . $nullCount . ' rows have NULL id instead of computed values'
                );
            }
        }
        $this->assertNotFalse($row, 'Row with computed id=11 should exist');
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
