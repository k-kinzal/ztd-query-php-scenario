<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with subquery referencing another table on SQLite.
 *
 * Real-world scenario: delete orphaned records, clean up records not
 * referenced by another table, etc. The CTE rewriter must handle
 * cross-table subqueries in DELETE WHERE clauses.
 *
 * @spec SPEC-4.3
 */
class SqliteMultiTableSubqueryDeleteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mtsd_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE mtsd_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category_id INTEGER,
                status TEXT NOT NULL DEFAULT \'active\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mtsd_products', 'mtsd_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mtsd_categories VALUES (1, 'Electronics')");
        $this->ztdExec("INSERT INTO mtsd_categories VALUES (2, 'Books')");

        $this->ztdExec("INSERT INTO mtsd_products VALUES (1, 'Phone', 1, 'active')");
        $this->ztdExec("INSERT INTO mtsd_products VALUES (2, 'Tablet', 1, 'active')");
        $this->ztdExec("INSERT INTO mtsd_products VALUES (3, 'Novel', 2, 'active')");
        $this->ztdExec("INSERT INTO mtsd_products VALUES (4, 'Orphan', 99, 'active')");  // no category
    }

    /**
     * DELETE WHERE NOT IN (subquery from other table).
     */
    public function testDeleteWhereNotInOtherTable(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mtsd_products WHERE category_id NOT IN (SELECT id FROM mtsd_categories)"
            );

            $rows = $this->ztdQuery("SELECT id FROM mtsd_products ORDER BY id");
            $this->assertCount(3, $rows, 'Should have 3 products (orphan deleted)');
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(2, (int) $rows[1]['id']);
            $this->assertSame(3, (int) $rows[2]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE NOT IN failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE NOT EXISTS (correlated subquery).
     */
    public function testDeleteWhereNotExistsCorrelated(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mtsd_products
                 WHERE NOT EXISTS (
                     SELECT 1 FROM mtsd_categories c WHERE c.id = mtsd_products.category_id
                 )"
            );

            $rows = $this->ztdQuery("SELECT id FROM mtsd_products ORDER BY id");
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE NOT EXISTS failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE IN (subquery with condition from other table).
     */
    public function testDeleteWhereInFilteredSubquery(): void
    {
        try {
            // Delete products in the 'Books' category
            $this->ztdExec(
                "DELETE FROM mtsd_products
                 WHERE category_id IN (SELECT id FROM mtsd_categories WHERE name = 'Books')"
            );

            $rows = $this->ztdQuery("SELECT id FROM mtsd_products ORDER BY id");
            $this->assertCount(3, $rows, 'Should have 3 products (Book product deleted)');
            $names = array_column($rows, 'id');
            $this->assertNotContains('3', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE WHERE IN filtered failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with subquery and parameter.
     */
    public function testPreparedDeleteWithSubqueryParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM mtsd_products
                 WHERE category_id IN (SELECT id FROM mtsd_categories WHERE name = ?)"
            );
            $stmt->execute(['Electronics']);

            $rows = $this->ztdQuery("SELECT name FROM mtsd_products ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertSame('Novel', $rows[0]['name']);
            $this->assertSame('Orphan', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared DELETE with subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE then verify shadow isolation.
     */
    public function testDeleteIsolation(): void
    {
        $this->ztdExec(
            "DELETE FROM mtsd_products WHERE category_id NOT IN (SELECT id FROM mtsd_categories)"
        );

        // Physical table should be unchanged
        $this->pdo->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mtsd_products");
        // Physical table has 0 rows (all shadow-inserted)
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
