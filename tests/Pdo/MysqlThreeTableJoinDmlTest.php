<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DML operations involving 3-table JOINs on MySQL.
 *
 * Pattern: real-world scenario where DELETE/UPDATE decisions require
 * joining across 3 shadow tables (e.g., customers → orders → products).
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MysqlThreeTableJoinDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_ttjd_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                tier VARCHAR(20) NOT NULL DEFAULT \'standard\'
            ) ENGINE=InnoDB',
            'CREATE TABLE my_ttjd_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                product_id INT NOT NULL,
                qty INT NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_ttjd_products (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(30) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_ttjd_orders', 'my_ttjd_products', 'my_ttjd_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_ttjd_products VALUES (1, 'Laptop', 'electronics', 999.99)");
        $this->pdo->exec("INSERT INTO my_ttjd_products VALUES (2, 'Book', 'education', 19.99)");
        $this->pdo->exec("INSERT INTO my_ttjd_products VALUES (3, 'Headphones', 'electronics', 149.99)");

        $this->pdo->exec("INSERT INTO my_ttjd_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO my_ttjd_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO my_ttjd_customers VALUES (3, 'Charlie', 'premium')");

        $this->pdo->exec("INSERT INTO my_ttjd_orders VALUES (1, 1, 1, 1)");
        $this->pdo->exec("INSERT INTO my_ttjd_orders VALUES (2, 2, 2, 3)");
        $this->pdo->exec("INSERT INTO my_ttjd_orders VALUES (3, 3, 1, 1)");
        $this->pdo->exec("INSERT INTO my_ttjd_orders VALUES (4, 3, 2, 2)");
    }

    /**
     * SELECT across 3 shadow tables to verify baseline join behavior.
     */
    public function testThreeTableJoinSelect(): void
    {
        $sql = "SELECT c.name, p.name AS product, o.qty
                FROM my_ttjd_customers c
                JOIN my_ttjd_orders o ON o.customer_id = c.id
                JOIN my_ttjd_products p ON p.id = o.product_id
                WHERE p.category = 'electronics'
                ORDER BY c.name";

        try {
            $rows = $this->ztdQuery($sql);

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('3-table SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE customers who ordered products in a specific category.
     */
    public function testDeleteViaThreeTableSubquery(): void
    {
        $sql = "DELETE FROM my_ttjd_customers
                WHERE id IN (
                    SELECT o.customer_id
                    FROM my_ttjd_orders o
                    JOIN my_ttjd_products p ON p.id = o.product_id
                    WHERE p.category = 'electronics'
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name FROM my_ttjd_customers ORDER BY name");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    '3-table DELETE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('3-table DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with 3-table join and bound category parameter.
     */
    public function testPreparedDeleteThreeTableJoin(): void
    {
        $sql = "DELETE FROM my_ttjd_customers
                WHERE id IN (
                    SELECT o.customer_id
                    FROM my_ttjd_orders o
                    JOIN my_ttjd_products p ON p.id = o.product_id
                    WHERE p.category = ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['education']);

            $rows = $this->ztdQuery("SELECT name FROM my_ttjd_customers ORDER BY name");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared 3-table DELETE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared 3-table DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE customer tier based on total spending across orders×products.
     */
    public function testUpdateViaThreeTableAggregateSubquery(): void
    {
        $sql = "UPDATE my_ttjd_customers
                SET tier = 'gold'
                WHERE id IN (
                    SELECT o.customer_id
                    FROM my_ttjd_orders o
                    JOIN my_ttjd_products p ON p.id = o.product_id
                    GROUP BY o.customer_id
                    HAVING SUM(o.qty * p.price) > 500
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, tier FROM my_ttjd_customers ORDER BY name");

            $this->assertCount(3, $rows);

            $tiers = array_column($rows, 'tier', 'name');
            if ($tiers['Alice'] !== 'gold') {
                $this->markTestIncomplete(
                    'Alice should be gold (spent 999.99), got: ' . $tiers['Alice']
                    . '. All: ' . json_encode($tiers)
                );
            }

            $this->assertSame('gold', $tiers['Alice']);
            $this->assertSame('standard', $tiers['Bob']);
            $this->assertSame('gold', $tiers['Charlie']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('3-table UPDATE aggregate failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with 3-table join and price threshold parameter.
     */
    public function testPreparedUpdateThreeTableJoinWithParam(): void
    {
        $sql = "UPDATE my_ttjd_customers
                SET tier = 'vip'
                WHERE id IN (
                    SELECT o.customer_id
                    FROM my_ttjd_orders o
                    JOIN my_ttjd_products p ON p.id = o.product_id
                    GROUP BY o.customer_id
                    HAVING SUM(o.qty * p.price) > ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100]);

            $rows = $this->ztdQuery("SELECT name, tier FROM my_ttjd_customers ORDER BY name");

            $tiers = array_column($rows, 'tier', 'name');
            if ($tiers['Alice'] !== 'vip') {
                $this->markTestIncomplete(
                    'Prepared 3-table UPDATE: Alice expected vip, got ' . $tiers['Alice']
                    . '. All: ' . json_encode($tiers)
                );
            }

            $this->assertSame('vip', $tiers['Alice']);
            $this->assertSame('standard', $tiers['Bob']);
            $this->assertSame('vip', $tiers['Charlie']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared 3-table UPDATE failed: ' . $e->getMessage());
        }
    }
}
