<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL UPDATE ... FROM syntax (multi-table update).
 *
 * PostgreSQL uses: UPDATE t1 SET col = t2.col FROM t2 WHERE t1.id = t2.id
 * This is different from MySQL's UPDATE ... JOIN syntax.
 * @spec SPEC-4.2c
 */
class PostgresMultiTableUpdateFromTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mtu_categories (id INT PRIMARY KEY, name VARCHAR(50), discount_pct INT DEFAULT 0)',
            'CREATE TABLE pg_mtu_products (id INT PRIMARY KEY, name VARCHAR(50), cat_id INT, price DECIMAL(10,2))',
            'CREATE TABLE pg_mtu_prices (product_id INT PRIMARY KEY, new_price DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mtu_prices', 'pg_mtu_products', 'pg_mtu_categories'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_mtu_categories VALUES (1, 'Electronics', 10)");
        $this->pdo->exec("INSERT INTO pg_mtu_categories VALUES (2, 'Books', 5)");
        $this->pdo->exec("INSERT INTO pg_mtu_products VALUES (1, 'Laptop', 1, 1000.00)");
        $this->pdo->exec("INSERT INTO pg_mtu_products VALUES (2, 'Phone', 1, 500.00)");
        $this->pdo->exec("INSERT INTO pg_mtu_products VALUES (3, 'Novel', 2, 20.00)");
        $this->pdo->exec("INSERT INTO pg_mtu_prices VALUES (1, 899.99)");
        $this->pdo->exec("INSERT INTO pg_mtu_prices VALUES (2, 449.99)");
    }

    /**
     * UPDATE ... FROM with price lookup table.
     */
    public function testUpdateFromPriceTable(): void
    {
        $this->pdo->exec(
            'UPDATE pg_mtu_products SET price = pg_mtu_prices.new_price
             FROM pg_mtu_prices
             WHERE pg_mtu_products.id = pg_mtu_prices.product_id'
        );

        $stmt = $this->pdo->query('SELECT price FROM pg_mtu_products WHERE id = 1');
        $price = (float) $stmt->fetchColumn();
        $this->assertEquals(899.99, $price);
    }

    /**
     * UPDATE ... FROM with category-based discount.
     */
    public function testUpdateFromCategoryDiscount(): void
    {
        $this->pdo->exec(
            "UPDATE pg_mtu_products SET price = pg_mtu_products.price * (1 - pg_mtu_categories.discount_pct / 100.0)
             FROM pg_mtu_categories
             WHERE pg_mtu_products.cat_id = pg_mtu_categories.id
             AND pg_mtu_categories.name = 'Electronics'"
        );

        $stmt = $this->pdo->query('SELECT price FROM pg_mtu_products WHERE id = 1');
        $price = (float) $stmt->fetchColumn();
        // 1000 * 0.90 = 900
        $this->assertEquals(900.00, $price);

        // Books not affected
        $stmt = $this->pdo->query('SELECT price FROM pg_mtu_products WHERE id = 3');
        $this->assertEquals(20.00, (float) $stmt->fetchColumn());
    }

    /**
     * UPDATE ... FROM doesn't affect unmatched rows.
     */
    public function testUpdateFromOnlyMatchedRows(): void
    {
        $this->pdo->exec(
            'UPDATE pg_mtu_products SET price = pg_mtu_prices.new_price
             FROM pg_mtu_prices
             WHERE pg_mtu_products.id = pg_mtu_prices.product_id'
        );

        // Product 3 (Novel) not in prices table — unchanged
        $stmt = $this->pdo->query('SELECT price FROM pg_mtu_products WHERE id = 3');
        $this->assertEquals(20.00, (float) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtu_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
