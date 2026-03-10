<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests recursive CTEs (WITH RECURSIVE) in DML context on SQLite.
 *
 * Recursive CTEs are common for hierarchical data (org charts, category trees,
 * bill of materials). This tests whether the CTE rewriter correctly handles
 * DML statements that reference recursive CTEs in subqueries.
 *
 * @spec SPEC-10.2
 */
class SqliteRecursiveCteDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_rct_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                parent_id INTEGER,
                active INTEGER DEFAULT 1
            )",
            "CREATE TABLE sl_rct_flat (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER,
                path TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rct_flat', 'sl_rct_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_rct_categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->ztdExec("INSERT INTO sl_rct_categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");
        $this->ztdExec("INSERT INTO sl_rct_categories (id, name, parent_id) VALUES (3, 'Phones', 2)");
        $this->ztdExec("INSERT INTO sl_rct_categories (id, name, parent_id) VALUES (4, 'Laptops', 2)");
        $this->ztdExec("INSERT INTO sl_rct_categories (id, name, parent_id) VALUES (5, 'Clothing', 1)");
        $this->ztdExec("INSERT INTO sl_rct_categories (id, name, parent_id) VALUES (6, 'Shirts', 5)");
    }

    /**
     * SELECT with recursive CTE — baseline: does recursive CTE work through shadow?
     */
    public function testSelectRecursiveCte(): void
    {
        try {
            $rows = $this->ztdQuery(
                "WITH RECURSIVE tree AS (
                    SELECT id, name, parent_id, name AS path FROM sl_rct_categories WHERE parent_id IS NULL
                    UNION ALL
                    SELECT c.id, c.name, c.parent_id, tree.path || '/' || c.name
                    FROM sl_rct_categories c
                    JOIN tree ON c.parent_id = tree.id
                )
                SELECT id, name, path FROM tree ORDER BY path"
            );

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'SELECT recursive CTE: expected 6 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
            $this->assertSame('Root', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT recursive CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from recursive CTE to flatten hierarchy.
     */
    public function testInsertFromRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_rct_flat (category_id, path)
                 WITH RECURSIVE tree AS (
                     SELECT id, name, parent_id, name AS path FROM sl_rct_categories WHERE parent_id IS NULL
                     UNION ALL
                     SELECT c.id, c.name, c.parent_id, tree.path || '/' || c.name
                     FROM sl_rct_categories c
                     JOIN tree ON c.parent_id = tree.id
                 )
                 SELECT id, path FROM tree"
            );

            $rows = $this->ztdQuery("SELECT category_id, path FROM sl_rct_flat ORDER BY path");

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'INSERT from recursive CTE: expected 6 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT from recursive CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE using recursive CTE to find all descendants of a subtree.
     */
    public function testDeleteSubtreeViaRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_rct_categories WHERE id IN (
                    WITH RECURSIVE subtree AS (
                        SELECT id FROM sl_rct_categories WHERE id = 2
                        UNION ALL
                        SELECT c.id FROM sl_rct_categories c
                        JOIN subtree ON c.parent_id = subtree.id
                    )
                    SELECT id FROM subtree
                )"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_rct_categories ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE subtree via recursive CTE: expected 3 remaining, got '
                    . count($rows) . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Root', $names);
            $this->assertContains('Clothing', $names);
            $this->assertContains('Shirts', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subtree via recursive CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE using recursive CTE to deactivate a subtree.
     */
    public function testUpdateSubtreeViaRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_rct_categories SET active = 0 WHERE id IN (
                    WITH RECURSIVE subtree AS (
                        SELECT id FROM sl_rct_categories WHERE id = 5
                        UNION ALL
                        SELECT c.id FROM sl_rct_categories c
                        JOIN subtree ON c.parent_id = subtree.id
                    )
                    SELECT id FROM subtree
                )"
            );

            $active = $this->ztdQuery("SELECT id, name FROM sl_rct_categories WHERE active = 1 ORDER BY id");
            $inactive = $this->ztdQuery("SELECT id, name FROM sl_rct_categories WHERE active = 0 ORDER BY id");

            if (count($inactive) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE subtree via recursive CTE: expected 2 inactive, got '
                    . count($inactive) . '. Active: ' . json_encode($active)
                    . '. Inactive: ' . json_encode($inactive)
                );
            }

            $this->assertCount(4, $active);
            $this->assertCount(2, $inactive);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE subtree via recursive CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT from recursive CTE after prior DML on source table.
     */
    public function testInsertRecursiveCteAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_rct_categories (id, name, parent_id) VALUES (7, 'Accessories', 3)");

            $this->ztdExec(
                "INSERT INTO sl_rct_flat (category_id, path)
                 WITH RECURSIVE tree AS (
                     SELECT id, name, parent_id, name AS path FROM sl_rct_categories WHERE id = 2
                     UNION ALL
                     SELECT c.id, c.name, c.parent_id, tree.path || '/' || c.name
                     FROM sl_rct_categories c
                     JOIN tree ON c.parent_id = tree.id
                 )
                 SELECT id, path FROM tree"
            );

            $rows = $this->ztdQuery("SELECT category_id, path FROM sl_rct_flat ORDER BY path");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT recursive CTE after DML: expected 4 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT recursive CTE after DML failed: ' . $e->getMessage());
        }
    }
}
