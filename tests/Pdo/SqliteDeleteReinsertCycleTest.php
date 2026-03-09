<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests delete-reinsert cycles and self-referencing subquery in UPDATE WHERE (SQLite PDO).
 * SQL patterns exercised: DELETE then re-INSERT same PK, UPDATE WHERE IN (SELECT from same table),
 * chained delete-reinsert-update on same PK, shadow store PK tracking integrity.
 * @spec SPEC-10.2.173
 */
class SqliteDeleteReinsertCycleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dri_product (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                status TEXT NOT NULL DEFAULT \'active\'
            )',
            'CREATE TABLE sl_dri_price_log (
                id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                old_price REAL,
                new_price REAL NOT NULL,
                changed_at TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dri_price_log', 'sl_dri_product'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (1, 'Widget A', 'electronics', 29.99, 'active')");
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (2, 'Widget B', 'electronics', 49.99, 'active')");
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (3, 'Gadget X', 'accessories', 9.99, 'active')");
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (4, 'Gadget Y', 'accessories', 14.99, 'discontinued')");
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (5, 'Tool Z', 'tools', 79.99, 'active')");

        $this->pdo->exec("INSERT INTO sl_dri_price_log VALUES (1, 1, 24.99, 29.99, '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_dri_price_log VALUES (2, 2, 44.99, 49.99, '2025-02-01')");
    }

    /**
     * DELETE a row then re-INSERT with same PK but different values.
     * Verifies shadow store correctly replaces the deleted row.
     */
    public function testDeleteThenReinsertSamePk(): void
    {
        // Delete product 3
        $affected = $this->ztdExec("DELETE FROM sl_dri_product WHERE id = 3");
        $this->assertEquals(1, $affected);

        // Verify it's gone
        $rows = $this->ztdQuery("SELECT * FROM sl_dri_product WHERE id = 3");
        $this->assertCount(0, $rows);

        // Re-insert with same PK, different values
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (3, 'Gadget X Pro', 'electronics', 19.99, 'active')");

        // Verify new values
        $rows = $this->ztdQuery("SELECT name, category, price FROM sl_dri_product WHERE id = 3");
        $this->assertCount(1, $rows);
        $this->assertSame('Gadget X Pro', $rows[0]['name']);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEqualsWithDelta(19.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * DELETE then re-INSERT, then UPDATE the re-inserted row.
     * Verifies chained operations on same PK work correctly.
     */
    public function testDeleteReinsertThenUpdate(): void
    {
        $this->ztdExec("DELETE FROM sl_dri_product WHERE id = 1");
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (1, 'Widget A v2', 'electronics', 34.99, 'active')");
        $this->ztdExec("UPDATE sl_dri_product SET price = 39.99 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, price FROM sl_dri_product WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Widget A v2', $rows[0]['name']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Total row count remains correct after delete-reinsert cycle.
     */
    public function testRowCountAfterDeleteReinsert(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dri_product");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->ztdExec("DELETE FROM sl_dri_product WHERE id = 4");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dri_product");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (4, 'Gadget Y Reborn', 'accessories', 16.99, 'active')");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dri_product");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * UPDATE WHERE IN (SELECT from same table) — self-referencing subquery.
     * Mark all electronics products as 'featured'.
     */
    public function testUpdateWhereInSelfReferencing(): void
    {
        $this->ztdExec(
            "UPDATE sl_dri_product SET status = 'featured'
             WHERE id IN (SELECT id FROM sl_dri_product WHERE category = 'electronics')"
        );

        $rows = $this->ztdQuery(
            "SELECT id, status FROM sl_dri_product WHERE category = 'electronics' ORDER BY id"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('featured', $rows[0]['status']);
        $this->assertSame('featured', $rows[1]['status']);

        // Non-electronics should be unchanged
        $rows = $this->ztdQuery(
            "SELECT id, status FROM sl_dri_product WHERE category != 'electronics' ORDER BY id"
        );
        foreach ($rows as $row) {
            $this->assertNotSame('featured', $row['status']);
        }
    }

    /**
     * UPDATE SET price = price * factor WHERE category matches subquery result.
     * 10% price increase for all products in the same category as the most expensive product.
     */
    public function testUpdateWithSelfReferencingCategorySubquery(): void
    {
        // Tools category has the most expensive product (Tool Z at 79.99)
        // This should increase prices for tools category products only
        $this->ztdExec(
            "UPDATE sl_dri_product SET price = ROUND(price * 1.10, 2)
             WHERE category = (SELECT category FROM sl_dri_product ORDER BY price DESC LIMIT 1)"
        );

        $rows = $this->ztdQuery("SELECT price FROM sl_dri_product WHERE id = 5");
        $this->assertEqualsWithDelta(87.99, (float) $rows[0]['price'], 0.01);

        // Electronics prices should be unchanged
        $rows = $this->ztdQuery("SELECT price FROM sl_dri_product WHERE id = 1");
        $this->assertEqualsWithDelta(29.99, (float) $rows[0]['price'], 0.01);
    }

    /**
     * DELETE WHERE NOT IN — keep only the cheapest product per category.
     * This is a common dedup / cleanup pattern.
     *
     * Known issue on SQLite: SPEC-11.UPDATE-AGGREGATE-SUBQUERY — CTE rewriter
     * truncates SQL with nested subqueries containing GROUP BY, causing "incomplete input".
     */
    public function testDeleteWhereNotInKeepCheapest(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_dri_product
                 WHERE id NOT IN (
                    SELECT MIN(id) FROM sl_dri_product
                    WHERE id IN (
                        SELECT id FROM sl_dri_product p2
                        WHERE price = (SELECT MIN(price) FROM sl_dri_product p3 WHERE p3.category = p2.category)
                    )
                    GROUP BY category
                 )"
            );

            $rows = $this->ztdQuery("SELECT id, name, category FROM sl_dri_product ORDER BY category");
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->assertStringContainsString('incomplete input', $e->getMessage());
            $this->markTestIncomplete(
                'SPEC-11.UPDATE-AGGREGATE-SUBQUERY: DELETE WHERE NOT IN with GROUP BY subquery fails on SQLite'
            );
        }
    }

    /**
     * Mixed exec() and prepare() on same data in same session.
     * Insert via exec, query via prepare.
     */
    public function testMixedExecAndPrepare(): void
    {
        // Insert via exec
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (6, 'New Item', 'electronics', 99.99, 'active')");

        // Query via prepare
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name, price FROM sl_dri_product WHERE category = ? AND price > ?",
            ['electronics', 40.00]
        );

        $this->assertGreaterThanOrEqual(2, count($rows));
        $names = array_column($rows, 'name');
        $this->assertContains('Widget B', $names);
        $this->assertContains('New Item', $names);
    }

    /**
     * Delete-reinsert cycle with JOIN query to verify cross-table consistency.
     */
    public function testDeleteReinsertWithJoinVerification(): void
    {
        // Product 1 has price log entries
        $this->ztdExec("DELETE FROM sl_dri_product WHERE id = 1");
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (1, 'Widget A Renewed', 'electronics', 35.99, 'active')");

        // Price log still references product 1 — JOIN should work with new product name
        $rows = $this->ztdQuery(
            "SELECT p.name, pl.old_price, pl.new_price
             FROM sl_dri_product p
             JOIN sl_dri_price_log pl ON pl.product_id = p.id
             WHERE p.id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Widget A Renewed', $rows[0]['name']);
        $this->assertEqualsWithDelta(24.99, (float) $rows[0]['old_price'], 0.01);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("DELETE FROM sl_dri_product WHERE id = 3");
        $this->pdo->exec("INSERT INTO sl_dri_product VALUES (3, 'Replaced', 'other', 1.00, 'active')");

        $rows = $this->ztdQuery("SELECT name FROM sl_dri_product WHERE id = 3");
        $this->assertSame('Replaced', $rows[0]['name']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_dri_product")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
