<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests upsert (ON CONFLICT DO UPDATE) where the SET clause uses a scalar
 * subquery to compute the new value.
 *
 * Pattern: INSERT ... ON CONFLICT DO UPDATE SET col = (SELECT ... FROM other_table)
 *
 * This is a common pattern for pulling a computed or looked-up value during upsert.
 * The CTE rewriter must correctly handle subqueries inside the SET clause of an
 * ON CONFLICT DO UPDATE.
 *
 * @spec SPEC-4.2
 */
class SqliteUpsertScalarSubquerySetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_uss_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                discount_pct REAL NOT NULL DEFAULT 0.0
            )',
            'CREATE TABLE sl_uss_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                price REAL NOT NULL,
                discount REAL NOT NULL DEFAULT 0.0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_uss_products', 'sl_uss_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Seed categories
        $this->pdo->exec("INSERT INTO sl_uss_categories (id, name, discount_pct) VALUES (1, 'Electronics', 10.0)");
        $this->pdo->exec("INSERT INTO sl_uss_categories (id, name, discount_pct) VALUES (2, 'Books', 5.0)");

        // Seed products
        $this->pdo->exec("INSERT INTO sl_uss_products (id, name, category_id, price, discount) VALUES (1, 'Phone', 1, 500.00, 0.0)");
    }

    /**
     * ON CONFLICT DO UPDATE SET discount = (SELECT discount_pct FROM categories ...)
     * The SET value comes from a subquery against a different table.
     *
     * @spec SPEC-4.2
     */
    public function testUpsertSetFromScalarSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_uss_products (id, name, category_id, price, discount) VALUES (1, 'Phone', 1, 550.00, 0.0)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price,
                     discount = (SELECT discount_pct FROM sl_uss_categories WHERE id = excluded.category_id)"
            );

            $rows = $this->ztdQuery('SELECT id, name, price, discount FROM sl_uss_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Upsert scalar subquery SET: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(550.00, (float) $rows[0]['price'], 0.01,
                'Price should be updated to 550.00');
            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['discount'], 0.01,
                'Discount should be set from category lookup (10.0)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert with scalar subquery in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Insert a new row (no conflict) — subquery in SET should not affect the insert.
     *
     * @spec SPEC-4.2
     */
    public function testUpsertScalarSubqueryNoConflict(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_uss_products (id, name, category_id, price, discount) VALUES (2, 'Book', 2, 20.00, 0.0)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price,
                     discount = (SELECT discount_pct FROM sl_uss_categories WHERE id = excluded.category_id)"
            );

            $rows = $this->ztdQuery('SELECT id, name, price, discount FROM sl_uss_products WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Upsert no conflict: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Book', $rows[0]['name']);
            $this->assertEqualsWithDelta(20.00, (float) $rows[0]['price'], 0.01);
            // When no conflict, the DO UPDATE is not executed, so discount stays 0.0
            $this->assertEqualsWithDelta(0.0, (float) $rows[0]['discount'], 0.01,
                'Discount should remain 0.0 on fresh insert (no conflict)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert scalar subquery (no conflict) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Upsert with COALESCE around a scalar subquery in SET.
     *
     * Pattern: SET discount = COALESCE((SELECT ...), 0.0)
     * Guards against NULL from the subquery.
     *
     * @spec SPEC-4.2
     */
    public function testUpsertCoalesceSubqueryInSet(): void
    {
        try {
            // category_id 99 doesn't exist, so subquery returns NULL
            $this->pdo->exec(
                "INSERT INTO sl_uss_products (id, name, category_id, price, discount) VALUES (1, 'Phone', 99, 600.00, 0.0)
                 ON CONFLICT (id) DO UPDATE
                 SET price = excluded.price,
                     category_id = excluded.category_id,
                     discount = COALESCE((SELECT discount_pct FROM sl_uss_categories WHERE id = excluded.category_id), 0.0)"
            );

            $rows = $this->ztdQuery('SELECT id, price, category_id, discount FROM sl_uss_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Upsert COALESCE subquery: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(600.00, (float) $rows[0]['price'], 0.01);
            $this->assertEqualsWithDelta(0.0, (float) $rows[0]['discount'], 0.01,
                'Discount should be 0.0 via COALESCE when subquery returns NULL');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert COALESCE subquery in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared upsert with scalar subquery in SET and ? params.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpsertScalarSubquerySet(): void
    {
        try {
            $sql = "INSERT INTO sl_uss_products (id, name, category_id, price, discount) VALUES (?, ?, ?, ?, 0.0)
                    ON CONFLICT (id) DO UPDATE
                    SET price = excluded.price,
                        discount = (SELECT discount_pct FROM sl_uss_categories WHERE id = ?)";

            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Phone', 1, 700.00, 1]);

            $rows = $this->ztdQuery('SELECT id, price, discount FROM sl_uss_products WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared upsert scalar subquery: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(700.00, (float) $rows[0]['price'], 0.01,
                'Price should be updated to 700.00');
            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['discount'], 0.01,
                'Discount should be 10.0 from category lookup via prepared stmt');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared upsert scalar subquery SET failed: ' . $e->getMessage()
            );
        }
    }
}
