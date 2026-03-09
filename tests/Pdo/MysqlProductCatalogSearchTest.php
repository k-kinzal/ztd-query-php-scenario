<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests product catalog with LIKE patterns, IN lists, NULLIF, COALESCE chains,
 * multi-column ORDER BY, LIMIT/OFFSET pagination, and string function round-trips
 * through the CTE shadow store (MySQL PDO).
 * SQL patterns exercised: LIKE, NOT LIKE, IN list, NULLIF, COALESCE chain,
 * multi-column ORDER BY, LIMIT OFFSET, CHAR_LENGTH, UPPER, prepared LIKE.
 * @spec SPEC-10.2.179
 */
class MysqlProductCatalogSearchTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_cat_product (
                id INT PRIMARY KEY,
                sku VARCHAR(20),
                name VARCHAR(100),
                category VARCHAR(50),
                brand VARCHAR(50),
                price DECIMAL(10,2),
                discount_price DECIMAL(10,2),
                description VARCHAR(500),
                is_active TINYINT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_cat_product'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_cat_product VALUES (1, 'WDG-001', 'Blue Widget', 'Widgets', 'Acme', 29.99, NULL, 'A standard blue widget', 1)");
        $this->pdo->exec("INSERT INTO mp_cat_product VALUES (2, 'WDG-002', 'Red Widget', 'Widgets', 'Acme', 34.99, 29.99, 'A premium red widget', 1)");
        $this->pdo->exec("INSERT INTO mp_cat_product VALUES (3, 'GDG-001', 'Gadget Pro', 'Gadgets', 'TechCo', 199.99, 179.99, 'Professional grade gadget', 1)");
        $this->pdo->exec("INSERT INTO mp_cat_product VALUES (4, 'GDG-002', 'Gadget Lite', 'Gadgets', 'TechCo', 99.99, NULL, 'Entry-level gadget', 1)");
        $this->pdo->exec("INSERT INTO mp_cat_product VALUES (5, 'WDG-003', 'Green Widget', 'Widgets', 'BestBrand', 24.99, 19.99, NULL, 1)");
        $this->pdo->exec("INSERT INTO mp_cat_product VALUES (6, 'ACC-001', 'Widget Case', 'Accessories', 'Acme', 9.99, NULL, 'Protective case for widgets', 0)");
        $this->pdo->exec("INSERT INTO mp_cat_product VALUES (7, 'ACC-002', 'Gadget Stand', 'Accessories', NULL, 14.99, NULL, 'Universal gadget stand', 1)");
    }

    public function testLikePrefix(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mp_cat_product WHERE sku LIKE 'WDG%' ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Blue Widget', $rows[0]['name']);
        $this->assertSame('Red Widget', $rows[1]['name']);
        $this->assertSame('Green Widget', $rows[2]['name']);
    }

    public function testLikeContains(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mp_cat_product WHERE name LIKE '%Widget%' ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Blue Widget', $rows[0]['name']);
        $this->assertSame('Widget Case', $rows[3]['name']);
    }

    public function testNotLike(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mp_cat_product
             WHERE name NOT LIKE '%Widget%' AND is_active = 1
             ORDER BY name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gadget Lite', $rows[0]['name']);
        $this->assertSame('Gadget Pro', $rows[1]['name']);
        $this->assertSame('Gadget Stand', $rows[2]['name']);
    }

    public function testInList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, category FROM mp_cat_product
             WHERE category IN ('Widgets', 'Accessories')
             ORDER BY category, name"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Accessories', $rows[0]['category']);
    }

    public function testNullifAndCoalesceChain(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    price,
                    discount_price,
                    COALESCE(discount_price, price) AS effective_price
             FROM mp_cat_product
             WHERE is_active = 1
             ORDER BY effective_price"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('Gadget Stand', $rows[0]['name']);
        $this->assertEqualsWithDelta(14.99, (float) $rows[0]['effective_price'], 0.01);
        $this->assertSame('Green Widget', $rows[1]['name']);
        $this->assertEqualsWithDelta(19.99, (float) $rows[1]['effective_price'], 0.01);
    }

    public function testCoalesceNullBrandAndDescription(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    COALESCE(brand, 'Unbranded') AS display_brand,
                    COALESCE(description, 'No description available') AS display_desc
             FROM mp_cat_product
             ORDER BY id"
        );

        $this->assertCount(7, $rows);
        $this->assertSame('No description available', $rows[4]['display_desc']);
        $this->assertSame('Unbranded', $rows[6]['display_brand']);
    }

    public function testMultiColumnOrderBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, category, price FROM mp_cat_product
             WHERE is_active = 1
             ORDER BY category ASC, price DESC"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('Accessories', $rows[0]['category']);
        $this->assertSame('Gadgets', $rows[1]['category']);
        $this->assertEqualsWithDelta(199.99, (float) $rows[1]['price'], 0.01);
    }

    public function testLimitOffsetPagination(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price FROM mp_cat_product
             WHERE is_active = 1
             ORDER BY price ASC
             LIMIT 3 OFFSET 3"
        );

        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(34.99, (float) $rows[0]['price'], 0.01);
    }

    public function testStringFunctions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    CHAR_LENGTH(name) AS name_len,
                    UPPER(category) AS cat_upper
             FROM mp_cat_product
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Blue Widget', $rows[0]['name']);
        $this->assertEquals(11, (int) $rows[0]['name_len']);
        $this->assertSame('WIDGETS', $rows[0]['cat_upper']);
    }

    public function testPreparedLike(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, category FROM mp_cat_product
             WHERE name LIKE ? AND is_active = ?
             ORDER BY name",
            ['%Gadget%', 1]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gadget Lite', $rows[0]['name']);
        $this->assertSame('Gadget Pro', $rows[1]['name']);
        $this->assertSame('Gadget Stand', $rows[2]['name']);
    }

    public function testUpdateWithLike(): void
    {
        $this->ztdExec(
            "UPDATE mp_cat_product SET price = price * 0.90
             WHERE category = 'Widgets' AND discount_price IS NULL"
        );

        $rows = $this->ztdQuery(
            "SELECT name, price FROM mp_cat_product WHERE category = 'Widgets' ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(26.99, (float) $rows[0]['price'], 0.01);
        $this->assertEqualsWithDelta(34.99, (float) $rows[1]['price'], 0.01);
    }

    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("UPDATE mp_cat_product SET price = 0 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT price FROM mp_cat_product WHERE id = 1");
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['price'], 0.01);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_cat_product")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
