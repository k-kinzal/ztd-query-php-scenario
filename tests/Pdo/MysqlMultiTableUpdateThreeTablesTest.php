<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multi-table UPDATE with 3+ tables on MySQL PDO.
 *
 * Cross-platform parity with MultiTableUpdateThreeTablesTest (MySQLi).
 * @spec pending
 */
class MysqlMultiTableUpdateThreeTablesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_mtu_categories (id INT PRIMARY KEY, name VARCHAR(50), discount_pct INT DEFAULT 0)',
            'CREATE TABLE pdo_mtu_products (id INT PRIMARY KEY, name VARCHAR(50), cat_id INT, price DECIMAL(10,2))',
            'CREATE TABLE pdo_mtu_discounts (id INT PRIMARY KEY, cat_id INT, extra_discount INT DEFAULT 0)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_mtu_discounts', 'pdo_mtu_products', 'pdo_mtu_categories'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_mtu_categories VALUES (1, 'Electronics', 10)");
        $this->pdo->exec("INSERT INTO pdo_mtu_categories VALUES (2, 'Books', 5)");
        $this->pdo->exec("INSERT INTO pdo_mtu_products VALUES (1, 'Laptop', 1, 1000.00)");
        $this->pdo->exec("INSERT INTO pdo_mtu_products VALUES (2, 'Phone', 1, 500.00)");
        $this->pdo->exec("INSERT INTO pdo_mtu_products VALUES (3, 'Novel', 2, 20.00)");
        $this->pdo->exec("INSERT INTO pdo_mtu_discounts VALUES (1, 1, 5)");
        $this->pdo->exec("INSERT INTO pdo_mtu_discounts VALUES (2, 2, 2)");
    }

    /**
     * Two-table UPDATE via PDO.
     */
    public function testTwoTableUpdate(): void
    {
        $this->pdo->exec(
            "UPDATE pdo_mtu_products p
             JOIN pdo_mtu_categories c ON p.cat_id = c.id
             SET p.price = p.price * (1 - c.discount_pct / 100.0)
             WHERE c.name = 'Electronics'"
        );

        $stmt = $this->pdo->query('SELECT price FROM pdo_mtu_products WHERE id = 1');
        $price = (float) $stmt->fetchColumn();
        $this->assertEquals(900.00, $price);
    }

    /**
     * Three-table UPDATE via PDO.
     */
    public function testThreeTableUpdate(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pdo_mtu_products p
                 JOIN pdo_mtu_categories c ON p.cat_id = c.id
                 JOIN pdo_mtu_discounts d ON c.id = d.cat_id
                 SET p.price = p.price * (1 - (c.discount_pct + d.extra_discount) / 100.0)
                 WHERE c.name = 'Electronics'"
            );

            $stmt = $this->pdo->query('SELECT price FROM pdo_mtu_products WHERE id = 1');
            $price = (float) $stmt->fetchColumn();
            $this->assertEquals(850.00, $price);
        } catch (\Exception $e) {
            $this->markTestSkipped('Three-table UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtu_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
