<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CASE WHEN expressions in various SQL contexts through CTE shadow store.
 *
 * Covers searched CASE, simple CASE, CASE in SELECT/ORDER BY/aggregate/UPDATE,
 * and NULL handling. Known issue: SPEC-11.CASE-WHERE-PARAMS (CASE-as-boolean
 * in WHERE with prepared params). These tests focus on other CASE usage patterns.
 */
class SqliteCaseExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_case_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            category TEXT NOT NULL,
            stock INTEGER
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_case_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_case_products VALUES (1, 'USB Cable', 5.99, 'Accessories', 200)");
        $this->pdo->exec("INSERT INTO sl_case_products VALUES (2, 'Keyboard', 49.99, 'Electronics', 30)");
        $this->pdo->exec("INSERT INTO sl_case_products VALUES (3, 'Monitor', 299.99, 'Electronics', 8)");
        $this->pdo->exec("INSERT INTO sl_case_products VALUES (4, 'Notebook', 3.50, 'Office', 500)");
        $this->pdo->exec("INSERT INTO sl_case_products VALUES (5, 'Desk Lamp', 24.99, 'Office', 0)");
        $this->pdo->exec("INSERT INTO sl_case_products VALUES (6, 'Webcam', 79.99, 'Electronics', NULL)");
    }

    /**
     * CASE WHEN in SELECT: classify products into price tiers.
     */
    public function testCaseInSelectPriceTier(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price,
                    CASE
                        WHEN price < 10 THEN 'cheap'
                        WHEN price < 100 THEN 'mid'
                        ELSE 'expensive'
                    END AS tier
             FROM sl_case_products
             ORDER BY id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('cheap', $rows[0]['tier']);      // USB Cable 5.99
        $this->assertSame('mid', $rows[1]['tier']);         // Keyboard 49.99
        $this->assertSame('expensive', $rows[2]['tier']);   // Monitor 299.99
        $this->assertSame('cheap', $rows[3]['tier']);       // Notebook 3.50
        $this->assertSame('mid', $rows[4]['tier']);         // Desk Lamp 24.99
        $this->assertSame('mid', $rows[5]['tier']);         // Webcam 79.99
    }

    /**
     * CASE WHEN in ORDER BY: custom sort order by category priority.
     * Electronics first, then Office, then Accessories.
     */
    public function testCaseInOrderBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, category
             FROM sl_case_products
             ORDER BY
                 CASE category
                     WHEN 'Electronics' THEN 1
                     WHEN 'Office' THEN 2
                     WHEN 'Accessories' THEN 3
                     ELSE 4
                 END,
                 name"
        );

        $this->assertCount(6, $rows);
        // Electronics first (alphabetical within group)
        $this->assertSame('Electronics', $rows[0]['category']);
        $this->assertSame('Electronics', $rows[1]['category']);
        $this->assertSame('Electronics', $rows[2]['category']);
        // Office next
        $this->assertSame('Office', $rows[3]['category']);
        $this->assertSame('Office', $rows[4]['category']);
        // Accessories last
        $this->assertSame('Accessories', $rows[5]['category']);
    }

    /**
     * CASE WHEN inside aggregate: conditional SUM per category.
     */
    public function testCaseInAggregateSumConditional(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                 SUM(CASE WHEN category = 'Electronics' THEN price ELSE 0 END) AS electronics_total,
                 SUM(CASE WHEN category = 'Office' THEN price ELSE 0 END) AS office_total,
                 SUM(CASE WHEN category = 'Accessories' THEN price ELSE 0 END) AS accessories_total
             FROM sl_case_products"
        );

        $this->assertCount(1, $rows);
        // Electronics: 49.99 + 299.99 + 79.99 = 429.97
        $this->assertEqualsWithDelta(429.97, (float) $rows[0]['electronics_total'], 0.01);
        // Office: 3.50 + 24.99 = 28.49
        $this->assertEqualsWithDelta(28.49, (float) $rows[0]['office_total'], 0.01);
        // Accessories: 5.99
        $this->assertEqualsWithDelta(5.99, (float) $rows[0]['accessories_total'], 0.01);
    }

    /**
     * Searched CASE with multiple WHEN branches (no base expression).
     * Classifies products by combined criteria.
     */
    public function testSearchedCaseMultipleBranches(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    CASE
                        WHEN price > 200 THEN 'premium'
                        WHEN price > 50 AND stock IS NOT NULL AND stock > 0 THEN 'standard-available'
                        WHEN price > 50 THEN 'standard-limited'
                        WHEN stock > 100 THEN 'budget-bulk'
                        ELSE 'budget'
                    END AS classification
             FROM sl_case_products
             ORDER BY id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('budget-bulk', $rows[0]['classification']);       // USB Cable: 5.99, stock 200
        $this->assertSame('budget', $rows[1]['classification']);            // Keyboard: 49.99, stock 30
        $this->assertSame('premium', $rows[2]['classification']);           // Monitor: 299.99
        $this->assertSame('budget-bulk', $rows[3]['classification']);       // Notebook: 3.50, stock 500
        $this->assertSame('budget', $rows[4]['classification']);            // Desk Lamp: 24.99, stock 0
        $this->assertSame('standard-limited', $rows[5]['classification']); // Webcam: 79.99, stock NULL
    }

    /**
     * Simple CASE (CASE <expr> WHEN <val> THEN ...): category display names.
     */
    public function testSimpleCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    CASE category
                        WHEN 'Electronics' THEN 'Tech & Gadgets'
                        WHEN 'Office' THEN 'Office Supplies'
                        WHEN 'Accessories' THEN 'Add-ons'
                        ELSE 'Other'
                    END AS display_category
             FROM sl_case_products
             ORDER BY id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('Add-ons', $rows[0]['display_category']);        // USB Cable
        $this->assertSame('Tech & Gadgets', $rows[1]['display_category']); // Keyboard
        $this->assertSame('Tech & Gadgets', $rows[2]['display_category']); // Monitor
        $this->assertSame('Office Supplies', $rows[3]['display_category']); // Notebook
        $this->assertSame('Office Supplies', $rows[4]['display_category']); // Desk Lamp
        $this->assertSame('Tech & Gadgets', $rows[5]['display_category']); // Webcam
    }

    /**
     * CASE WHEN with NULL handling: stock IS NULL branch.
     */
    public function testCaseWithNullHandling(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name,
                    CASE
                        WHEN stock IS NULL THEN 'unknown'
                        WHEN stock = 0 THEN 'out-of-stock'
                        WHEN stock < 10 THEN 'low'
                        WHEN stock < 50 THEN 'normal'
                        ELSE 'high'
                    END AS availability
             FROM sl_case_products
             ORDER BY id"
        );

        $this->assertCount(6, $rows);
        $this->assertSame('high', $rows[0]['availability']);       // USB Cable: stock 200
        $this->assertSame('normal', $rows[1]['availability']);     // Keyboard: stock 30
        $this->assertSame('low', $rows[2]['availability']);        // Monitor: stock 8
        $this->assertSame('high', $rows[3]['availability']);       // Notebook: stock 500
        $this->assertSame('out-of-stock', $rows[4]['availability']); // Desk Lamp: stock 0
        $this->assertSame('unknown', $rows[5]['availability']);    // Webcam: stock NULL
    }

    /**
     * CASE WHEN in UPDATE SET clause: apply discount to high-stock items.
     * Items with stock > 10 get 10% off; others keep their price.
     */
    public function testCaseInUpdateSet(): void
    {
        $this->pdo->exec(
            "UPDATE sl_case_products
             SET price = CASE
                 WHEN stock > 10 THEN price * 0.9
                 ELSE price
             END"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name, price, stock
             FROM sl_case_products
             ORDER BY id"
        );

        $this->assertCount(6, $rows);
        // Discounted (stock > 10)
        $this->assertEqualsWithDelta(5.391, (float) $rows[0]['price'], 0.01);  // USB Cable: 5.99 * 0.9
        $this->assertEqualsWithDelta(44.991, (float) $rows[1]['price'], 0.01); // Keyboard: 49.99 * 0.9
        $this->assertEqualsWithDelta(3.15, (float) $rows[3]['price'], 0.01);   // Notebook: 3.50 * 0.9

        // Not discounted (stock <= 10 or NULL)
        $this->assertEqualsWithDelta(299.99, (float) $rows[2]['price'], 0.01); // Monitor: stock 8
        $this->assertEqualsWithDelta(24.99, (float) $rows[4]['price'], 0.01);  // Desk Lamp: stock 0
        $this->assertEqualsWithDelta(79.99, (float) $rows[5]['price'], 0.01);  // Webcam: stock NULL
    }

    /**
     * Physical isolation: mutations through ZTD do not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_case_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
