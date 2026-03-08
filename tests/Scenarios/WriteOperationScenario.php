<?php

declare(strict_types=1);

namespace Tests\Scenarios;

/**
 * Shared write operation scenario for all platforms.
 *
 * Requires table: products (id INT/INTEGER PRIMARY KEY, name VARCHAR/TEXT, price INT/INTEGER)
 * Provided by the concrete test class via getTableDDL().
 */
trait WriteOperationScenario
{
    abstract protected function ztdExec(string $sql): int|false;
    abstract protected function ztdQuery(string $sql): array;
    abstract protected function ztdPrepareAndExecute(string $sql, array $params): array;
    abstract protected function disableZtd(): void;
    abstract protected function enableZtd(): void;

    public function testMultiRowInsert(): void
    {
        $this->ztdExec(
            "INSERT INTO products (id, name, price) VALUES "
            . "(1, 'Widget', 100), "
            . "(2, 'Gadget', 200), "
            . "(3, 'Doohickey', 50)"
        );

        $rows = $this->ztdQuery('SELECT * FROM products ORDER BY id');

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame(100, (int) $rows[0]['price']);
        $this->assertSame('Doohickey', $rows[2]['name']);
    }

    public function testBatchInsertInLoop(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec(
                "INSERT INTO products (id, name, price) VALUES ({$i}, 'Product{$i}', " . ($i * 10) . ")"
            );
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM products');
        $this->assertSame(10, (int) $rows[0]['cnt']);
    }

    public function testNullInsertAndQuery(): void
    {
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (1, NULL, NULL)");

        $rows = $this->ztdQuery('SELECT * FROM products WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
    }

    public function testNullComparisonIsNull(): void
    {
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)");
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (2, NULL, NULL)");

        $rows = $this->ztdQuery('SELECT * FROM products WHERE name IS NULL');
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);

        $rows = $this->ztdQuery('SELECT * FROM products WHERE name IS NOT NULL');
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testAggregateOverShadowStore(): void
    {
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (1, 'A', 100)");
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (2, 'B', 200)");
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (3, 'C', 300)");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(price) AS total FROM products');
        $this->assertSame(3, (int) $rows[0]['cnt']);
        $this->assertSame(600, (int) $rows[0]['total']);
    }

    public function testUpdateMultipleRows(): void
    {
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (1, 'A', 100)");
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (2, 'B', 200)");
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (3, 'C', 300)");

        $this->ztdExec("UPDATE products SET price = 0 WHERE price < 250");

        $rows = $this->ztdQuery('SELECT * FROM products WHERE price = 0 ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['name']);
        $this->assertSame('B', $rows[1]['name']);
    }

    public function testWriteIsolation(): void
    {
        $this->ztdExec("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)");

        // Verify shadow store has the data
        $rows = $this->ztdQuery('SELECT * FROM products');
        $this->assertCount(1, $rows);

        // Physical table should be empty
        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT * FROM products');
        $this->assertCount(0, $rows);
        $this->enableZtd();
    }

    public function testPreparedInsertAndVerify(): void
    {
        $this->ztdPrepareAndExecute(
            'INSERT INTO products (id, name, price) VALUES (?, ?, ?)',
            [1, 'Widget', 100]
        );

        $rows = $this->ztdQuery('SELECT * FROM products WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }
}
