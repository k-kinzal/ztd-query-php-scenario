<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests dynamic WHERE clause building patterns common in PHP applications:
 * optional filters, dynamic ORDER BY, parameterized LIMIT/OFFSET, NULL checks.
 * @spec SPEC-3.2
 */
class MysqlDynamicFilterBuildingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_dfb_products (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(50),
            price DECIMAL(10,2) NOT NULL,
            stock INT NOT NULL,
            discontinued TINYINT(1) DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_dfb_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (1, 'Laptop Pro', 'electronics', 1299.99, 25, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (2, 'Wireless Mouse', 'electronics', 29.99, 150, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (3, 'USB Cable', 'accessories', 9.99, 500, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (4, 'Standing Desk', 'furniture', 599.99, 12, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (5, 'Monitor 27in', 'electronics', 449.99, 40, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (6, 'Keyboard', 'electronics', 79.99, 200, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (7, 'Desk Lamp', 'furniture', 45.99, 80, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (8, 'Phone Case', 'accessories', 19.99, 300, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (9, 'Webcam HD', 'electronics', 89.99, 60, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (10, 'Headphones', 'electronics', 199.99, 35, 1)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (11, 'Chair Ergonomic', 'furniture', 899.99, 8, 0)");
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (12, 'Screen Protector', 'accessories', 14.99, 1000, 0)");
    }

    /**
     * WHERE 1=1 AND ... pattern -- the classic query builder approach.
     */
    public function testWhere1Equals1Pattern(): void
    {
        $sql = "SELECT name, price FROM mp_dfb_products WHERE 1=1 AND category = 'electronics' AND price < 100 ORDER BY price";
        $rows = $this->ztdQuery($sql);

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Mouse', $rows[0]['name']);
        $this->assertSame('Keyboard', $rows[1]['name']);
        $this->assertSame('Webcam HD', $rows[2]['name']);
    }

    /**
     * Filter by category only (price filter omitted).
     */
    public function testOptionalFilterCategoryOnly(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM mp_dfb_products WHERE 1=1 AND category = ? ORDER BY name",
            ['furniture']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Chair Ergonomic', $rows[0]['name']);
        $this->assertSame('Desk Lamp', $rows[1]['name']);
        $this->assertSame('Standing Desk', $rows[2]['name']);
    }

    /**
     * Filter by price only (category filter omitted).
     */
    public function testOptionalFilterPriceOnly(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, price FROM mp_dfb_products WHERE 1=1 AND price <= ? ORDER BY price",
            [20.00]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('USB Cable', $rows[0]['name']);
        $this->assertSame('Screen Protector', $rows[1]['name']);
        $this->assertSame('Phone Case', $rows[2]['name']);
    }

    /**
     * Filter by both category AND price range.
     */
    public function testOptionalFilterBothCategoryAndPrice(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, price FROM mp_dfb_products WHERE 1=1 AND category = ? AND price BETWEEN ? AND ? ORDER BY price",
            ['electronics', 50.00, 500.00]
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Keyboard', $rows[0]['name']);
        $this->assertSame('Webcam HD', $rows[1]['name']);
        $this->assertSame('Headphones', $rows[2]['name']);
        $this->assertSame('Monitor 27in', $rows[3]['name']);
    }

    /**
     * No optional filters applied -- return all rows.
     */
    public function testOptionalFilterNeitherApplied(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mp_dfb_products WHERE 1=1",
            []
        );

        $this->assertSame(12, (int) $rows[0]['cnt']);
    }

    /**
     * Dynamic ORDER BY -- sort by name ascending.
     */
    public function testDynamicOrderByNameAsc(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mp_dfb_products WHERE category = 'accessories' ORDER BY name ASC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Phone Case', $rows[0]['name']);
        $this->assertSame('Screen Protector', $rows[1]['name']);
        $this->assertSame('USB Cable', $rows[2]['name']);
    }

    /**
     * Dynamic ORDER BY -- sort by price descending.
     */
    public function testDynamicOrderByPriceDesc(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price FROM mp_dfb_products WHERE category = 'accessories' ORDER BY price DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Phone Case', $rows[0]['name']);
        $this->assertSame('Screen Protector', $rows[1]['name']);
        $this->assertSame('USB Cable', $rows[2]['name']);
    }

    /**
     * Dynamic LIMIT/OFFSET with optional category filter.
     * MySQL PDO requires PDO::PARAM_INT binding for LIMIT/OFFSET.
     */
    public function testDynamicLimitOffsetWithFilter(): void
    {
        $stmt = $this->ztdPrepare(
            "SELECT name FROM mp_dfb_products WHERE category = ? ORDER BY id LIMIT ? OFFSET ?"
        );

        // Page 1 of electronics (3 per page)
        $stmt->bindValue(1, 'electronics', PDO::PARAM_STR);
        $stmt->bindValue(2, 3, PDO::PARAM_INT);
        $stmt->bindValue(3, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $page1);
        $this->assertSame('Laptop Pro', $page1[0]['name']);
        $this->assertSame('Wireless Mouse', $page1[1]['name']);
        $this->assertSame('Monitor 27in', $page1[2]['name']);
    }

    /**
     * Prepared statement with different parameter counts -- use separate queries.
     */
    public function testSeparateQueriesForVaryingParams(): void
    {
        // Query 1: filter by category only
        $rows1 = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mp_dfb_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(6, (int) $rows1[0]['cnt']);

        // Query 2: filter by category + price range (different param count)
        $rows2 = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mp_dfb_products WHERE category = ? AND price > ?",
            ['electronics', 100.00]
        );
        $this->assertSame(3, (int) $rows2[0]['cnt']);

        // Query 3: no filter
        $rows3 = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mp_dfb_products WHERE 1=1",
            []
        );
        $this->assertSame(12, (int) $rows3[0]['cnt']);
    }

    /**
     * Filter with IS NULL check (find rows where category is NULL).
     */
    public function testFilterWithIsNull(): void
    {
        // Insert a product with NULL category
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (13, 'Mystery Item', NULL, 5.00, 1, 0)");

        $rows = $this->ztdQuery(
            "SELECT name FROM mp_dfb_products WHERE category IS NULL"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Mystery Item', $rows[0]['name']);
    }

    /**
     * Filter with IS NOT NULL check (exclude rows where category is NULL).
     */
    public function testFilterWithIsNotNull(): void
    {
        $this->pdo->exec("INSERT INTO mp_dfb_products VALUES (13, 'Mystery Item', NULL, 5.00, 1, 0)");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mp_dfb_products WHERE category IS NOT NULL"
        );

        // 12 original products all have category set
        $this->assertSame(12, (int) $rows[0]['cnt']);
    }

    /**
     * Filter with discontinued flag as boolean-like integer.
     */
    public function testFilterWithBooleanLikeColumn(): void
    {
        $activeRows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mp_dfb_products WHERE discontinued = 0"
        );
        $this->assertSame(11, (int) $activeRows[0]['cnt']);

        $discontinuedRows = $this->ztdQuery(
            "SELECT name FROM mp_dfb_products WHERE discontinued = 1"
        );
        $this->assertCount(1, $discontinuedRows);
        $this->assertSame('Headphones', $discontinuedRows[0]['name']);
    }

    /**
     * Physical isolation -- shadow data does not reach physical table.
     */
    public function testPhysicalIsolation(): void
    {
        // Verify data exists in ZTD mode
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_dfb_products");
        $this->assertSame(12, (int) $rows[0]['cnt']);

        // Disable ZTD and verify physical table is empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM mp_dfb_products");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
