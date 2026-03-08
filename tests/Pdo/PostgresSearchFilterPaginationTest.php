<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests parameterized search with multiple filter conditions and pagination,
 * simulating common REST API query patterns through ZTD shadow store (PostgreSQL PDO).
 * @spec SPEC-3.2
 */
class PostgresSearchFilterPaginationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sfp_products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            price NUMERIC(10,2),
            stock INTEGER,
            active INTEGER
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_sfp_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (1, 'Wireless Mouse', 'electronics', 29.99, 150, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (2, 'Wireless Keyboard', 'electronics', 49.99, 80, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (3, 'USB Cable', 'accessories', 9.99, 500, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (4, 'Monitor Stand', 'accessories', 39.99, 30, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (5, 'Laptop Bag', 'accessories', 59.99, 0, 0)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (6, 'Wireless Headphones', 'electronics', 89.99, 45, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (7, 'Desk Lamp', 'furniture', 24.99, 60, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (8, 'Standing Desk', 'furniture', 299.99, 10, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (9, 'Mouse Pad', 'accessories', 14.99, 200, 1)");
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (10, 'Webcam', 'electronics', 69.99, 25, 1)");
    }

    public function testSearchByNamePattern(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM pg_sfp_products WHERE name LIKE ? ORDER BY name",
            ['%Wireless%']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Headphones', $rows[0]['name']);
        $this->assertSame('Wireless Keyboard', $rows[1]['name']);
        $this->assertSame('Wireless Mouse', $rows[2]['name']);
    }

    public function testCategoryAndPriceRangeFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, price FROM pg_sfp_products
             WHERE category = ? AND price BETWEEN ? AND ?
             ORDER BY price",
            ['electronics', '30.00', '100.00']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Keyboard', $rows[0]['name']);
        $this->assertSame('Webcam', $rows[1]['name']);
        $this->assertSame('Wireless Headphones', $rows[2]['name']);
    }

    public function testActiveInStockFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, stock FROM pg_sfp_products
             WHERE active = ? AND stock > ?
             ORDER BY stock DESC",
            [1, 0]
        );

        $this->assertCount(9, $rows); // 10 total - 1 inactive (Laptop Bag)
        $this->assertSame('USB Cable', $rows[0]['name']);
    }

    public function testMultiFilterSearch(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, price FROM pg_sfp_products
             WHERE name LIKE ?
               AND category = ?
               AND price < ?
               AND active = ?
             ORDER BY price DESC",
            ['%Mouse%', 'accessories', '20.00', 1]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Mouse Pad', $rows[0]['name']);
    }

    public function testPaginatedResults(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name, price FROM pg_sfp_products
             WHERE active = 1
             ORDER BY price ASC
             LIMIT 3 OFFSET 0"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('USB Cable', $rows[0]['name']);
        $this->assertSame('Mouse Pad', $rows[1]['name']);
        $this->assertSame('Desk Lamp', $rows[2]['name']);

        $rows = $this->ztdQuery(
            "SELECT id, name, price FROM pg_sfp_products
             WHERE active = 1
             ORDER BY price ASC
             LIMIT 3 OFFSET 3"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Mouse', $rows[0]['name']);
        $this->assertSame('Monitor Stand', $rows[1]['name']);
        $this->assertSame('Wireless Keyboard', $rows[2]['name']);
    }

    public function testSearchAfterMutations(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM pg_sfp_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->exec(
            "INSERT INTO pg_sfp_products VALUES (11, 'Bluetooth Speaker', 'electronics', 44.99, 75, 1)"
        );

        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM pg_sfp_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(5, (int) $rows[0]['cnt']);
    }

    public function testIlikeCaseInsensitiveSearch(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM pg_sfp_products WHERE name ILIKE ? ORDER BY name",
            ['%wireless%']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Headphones', $rows[0]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sfp_products VALUES (11, 'New', 'test', 1.00, 1, 1)");
        $this->pdo->exec("UPDATE pg_sfp_products SET price = 999.99 WHERE id = 1");

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_sfp_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
