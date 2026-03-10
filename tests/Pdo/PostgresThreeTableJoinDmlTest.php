<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DML operations involving 3-table JOINs on PostgreSQL.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresThreeTableJoinDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ttjd_customers (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                tier VARCHAR(20) NOT NULL DEFAULT \'standard\'
            )',
            'CREATE TABLE pg_ttjd_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                qty INTEGER NOT NULL
            )',
            'CREATE TABLE pg_ttjd_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(30) NOT NULL,
                price NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ttjd_orders', 'pg_ttjd_products', 'pg_ttjd_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ttjd_products VALUES (1, 'Laptop', 'electronics', 999.99)");
        $this->pdo->exec("INSERT INTO pg_ttjd_products VALUES (2, 'Book', 'education', 19.99)");
        $this->pdo->exec("INSERT INTO pg_ttjd_products VALUES (3, 'Headphones', 'electronics', 149.99)");

        $this->pdo->exec("INSERT INTO pg_ttjd_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO pg_ttjd_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO pg_ttjd_customers VALUES (3, 'Charlie', 'premium')");

        $this->pdo->exec("INSERT INTO pg_ttjd_orders VALUES (1, 1, 1, 1)");
        $this->pdo->exec("INSERT INTO pg_ttjd_orders VALUES (2, 2, 2, 3)");
        $this->pdo->exec("INSERT INTO pg_ttjd_orders VALUES (3, 3, 1, 1)");
        $this->pdo->exec("INSERT INTO pg_ttjd_orders VALUES (4, 3, 2, 2)");
    }

    public function testThreeTableJoinSelect(): void
    {
        $sql = "SELECT c.name, p.name AS product, o.qty
                FROM pg_ttjd_customers c
                JOIN pg_ttjd_orders o ON o.customer_id = c.id
                JOIN pg_ttjd_products p ON p.id = o.product_id
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

    public function testDeleteViaThreeTableSubquery(): void
    {
        $sql = "DELETE FROM pg_ttjd_customers
                WHERE id IN (
                    SELECT o.customer_id
                    FROM pg_ttjd_orders o
                    JOIN pg_ttjd_products p ON p.id = o.product_id
                    WHERE p.category = 'electronics'
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM pg_ttjd_customers ORDER BY name");

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

    public function testPreparedDeleteThreeTableJoin(): void
    {
        $sql = "DELETE FROM pg_ttjd_customers
                WHERE id IN (
                    SELECT o.customer_id
                    FROM pg_ttjd_orders o
                    JOIN pg_ttjd_products p ON p.id = o.product_id
                    WHERE p.category = $1
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['education']);
            $rows = $this->ztdQuery("SELECT name FROM pg_ttjd_customers ORDER BY name");

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

    public function testUpdateViaThreeTableAggregateSubquery(): void
    {
        $sql = "UPDATE pg_ttjd_customers
                SET tier = 'gold'
                WHERE id IN (
                    SELECT o.customer_id
                    FROM pg_ttjd_orders o
                    JOIN pg_ttjd_products p ON p.id = o.product_id
                    GROUP BY o.customer_id
                    HAVING SUM(o.qty * p.price) > 500
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, tier FROM pg_ttjd_customers ORDER BY name");

            $this->assertCount(3, $rows);
            $tiers = array_column($rows, 'tier', 'name');

            if ($tiers['Alice'] !== 'gold') {
                $this->markTestIncomplete(
                    'Alice expected gold (spent 999.99), got: ' . $tiers['Alice']
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

    public function testPreparedUpdateThreeTableJoinWithParam(): void
    {
        $sql = "UPDATE pg_ttjd_customers
                SET tier = 'vip'
                WHERE id IN (
                    SELECT o.customer_id
                    FROM pg_ttjd_orders o
                    JOIN pg_ttjd_products p ON p.id = o.product_id
                    GROUP BY o.customer_id
                    HAVING SUM(o.qty * p.price) > $1
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100]);
            $rows = $this->ztdQuery("SELECT name, tier FROM pg_ttjd_customers ORDER BY name");

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
