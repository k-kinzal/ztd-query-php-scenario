<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests parameterized search with multiple filter conditions and pagination,
 * simulating common REST API query patterns through ZTD shadow store (MySQL PDO).
 * @spec SPEC-3.2
 */
class MysqlSearchFilterPaginationTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_sfp_products (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(50),
            price DECIMAL(10,2),
            stock INT,
            active TINYINT(1)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_sfp_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (1, 'Wireless Mouse', 'electronics', 29.99, 150, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (2, 'Wireless Keyboard', 'electronics', 49.99, 80, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (3, 'USB Cable', 'accessories', 9.99, 500, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (4, 'Monitor Stand', 'accessories', 39.99, 30, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (5, 'Laptop Bag', 'accessories', 59.99, 0, 0)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (6, 'Wireless Headphones', 'electronics', 89.99, 45, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (7, 'Desk Lamp', 'furniture', 24.99, 60, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (8, 'Standing Desk', 'furniture', 299.99, 10, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (9, 'Mouse Pad', 'accessories', 14.99, 200, 1)");
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (10, 'Webcam', 'electronics', 69.99, 25, 1)");
    }

    public function testSearchByNamePattern(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM mp_sfp_products WHERE name LIKE ? ORDER BY name",
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
            "SELECT id, name, price FROM mp_sfp_products
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
            "SELECT id, name, stock FROM mp_sfp_products
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
            "SELECT id, name, price FROM mp_sfp_products
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

    public function testPaginatedResultsWithBindValue(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT id, name, price FROM mp_sfp_products
             WHERE active = ?
             ORDER BY price ASC
             LIMIT ? OFFSET ?"
        );
        $stmt->bindValue(1, 1, PDO::PARAM_INT);
        $stmt->bindValue(2, 3, PDO::PARAM_INT);
        $stmt->bindValue(3, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $page1);
        $this->assertSame('USB Cable', $page1[0]['name']);

        $stmt->bindValue(1, 1, PDO::PARAM_INT);
        $stmt->bindValue(2, 3, PDO::PARAM_INT);
        $stmt->bindValue(3, 3, PDO::PARAM_INT);
        $stmt->execute();
        $page2 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $page2);
        $this->assertSame('Wireless Mouse', $page2[0]['name']);
    }

    public function testSearchAfterMutations(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mp_sfp_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->exec(
            "INSERT INTO mp_sfp_products VALUES (11, 'Bluetooth Speaker', 'electronics', 44.99, 75, 1)"
        );

        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mp_sfp_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(5, (int) $rows[0]['cnt']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_sfp_products VALUES (11, 'New', 'test', 1.00, 1, 1)");
        $this->pdo->exec("UPDATE mp_sfp_products SET price = 999.99 WHERE id = 1");

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_sfp_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
