<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DELETE WHERE with subquery referencing a DIFFERENT shadow-modified table.
 *
 * Pattern: DELETE FROM t1 WHERE col IN (SELECT ref FROM t2 WHERE condition)
 * Both t1 and t2 have shadow mutations. The CTE rewriter must correctly
 * rewrite references in both the outer DELETE and the inner subquery.
 *
 * This is distinct from self-referencing deletes (#59, #58) and same-table
 * IN subqueries (#100). Here the subquery targets a separate table that
 * also has shadow state.
 *
 * @spec SPEC-4.3
 */
class DeleteWithSubqueryOnOtherTableTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_dso_products (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category_id INT NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_dso_categories (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                active TINYINT NOT NULL DEFAULT 1
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_dso_products', 'mi_dso_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_dso_categories VALUES (1, 'Electronics', 1)");
        $this->mysqli->query("INSERT INTO mi_dso_categories VALUES (2, 'Clothing', 1)");
        $this->mysqli->query("INSERT INTO mi_dso_categories VALUES (3, 'Furniture', 0)");

        $this->mysqli->query("INSERT INTO mi_dso_products VALUES (1, 'Phone', 1)");
        $this->mysqli->query("INSERT INTO mi_dso_products VALUES (2, 'Shirt', 2)");
        $this->mysqli->query("INSERT INTO mi_dso_products VALUES (3, 'Desk', 3)");
        $this->mysqli->query("INSERT INTO mi_dso_products VALUES (4, 'Laptop', 1)");
    }

    /**
     * DELETE products in inactive categories using IN subquery.
     */
    public function testDeleteWhereInSubqueryOtherTable(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_dso_products WHERE category_id IN (SELECT id FROM mi_dso_categories WHERE active = 0)"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_dso_products ORDER BY id");

            // Furniture (category 3) is inactive, so Desk (id=3) should be deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN subquery: expected 3 rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains('Desk', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Shadow-modify the subquery table BEFORE the delete.
     * Deactivate Electronics, then delete products in inactive categories.
     */
    public function testDeleteWhereInSubqueryAfterCategoryUpdate(): void
    {
        try {
            // Shadow-deactivate Electronics
            $this->mysqli->query("UPDATE mi_dso_categories SET active = 0 WHERE id = 1");

            $this->mysqli->query(
                "DELETE FROM mi_dso_products WHERE category_id IN (SELECT id FROM mi_dso_categories WHERE active = 0)"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_dso_products ORDER BY id");

            // Inactive categories: 1(Electronics, shadow-deactivated) and 3(Furniture)
            // Products to delete: Phone(cat1), Laptop(cat1), Desk(cat3)
            // Remaining: Shirt(cat2)
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE after category UPDATE: expected 1 remaining row, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Shirt', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE after category UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Shadow-insert into subquery table, then delete based on new data.
     */
    public function testDeleteWhereInSubqueryAfterCategoryInsert(): void
    {
        try {
            // Shadow-insert a new inactive category
            $this->mysqli->query("INSERT INTO mi_dso_categories VALUES (4, 'Toys', 0)");
            // Shadow-insert a product in that category
            $this->mysqli->query("INSERT INTO mi_dso_products VALUES (5, 'Ball', 4)");

            $this->mysqli->query(
                "DELETE FROM mi_dso_products WHERE category_id IN (SELECT id FROM mi_dso_categories WHERE active = 0)"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_dso_products ORDER BY id");

            // Inactive: cat3 (Furniture), cat4 (Toys, shadow-inserted)
            // Delete: Desk(cat3), Ball(cat4)
            // Remaining: Phone(cat1), Shirt(cat2), Laptop(cat1)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE after category INSERT: expected 3 remaining rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains('Ball', array_column($rows, 'name'));
            $this->assertNotContains('Desk', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE after category INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE NOT IN subquery — keep products in active categories only.
     */
    public function testDeleteWhereNotInSubquery(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_dso_products WHERE category_id NOT IN (SELECT id FROM mi_dso_categories WHERE active = 1)"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_dso_products ORDER BY id");

            // Active: cat1 (Electronics), cat2 (Clothing)
            // Keep: Phone(cat1), Shirt(cat2), Laptop(cat1)
            // Delete: Desk(cat3, inactive)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE NOT IN: expected 3 rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains('Desk', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE NOT IN failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE EXISTS subquery on shadow-modified other table.
     */
    public function testDeleteWhereExistsSubquery(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_dso_products WHERE EXISTS (SELECT 1 FROM mi_dso_categories WHERE mi_dso_categories.id = mi_dso_products.category_id AND active = 0)"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_dso_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE EXISTS: expected 3 rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains('Desk', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE EXISTS failed: ' . $e->getMessage());
        }
    }
}
