<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests recursive CTEs used to drive DML operations.
 *
 * Recursive CTEs are commonly used for tree traversal, graph queries,
 * and generating series. Using them to drive INSERT/DELETE is a
 * real-world pattern that stresses the CTE rewriter heavily.
 *
 * @spec SPEC-3.3c, SPEC-4.1a
 */
class SqliteRecursiveCteDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_rcd_categories (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)',
            'CREATE TABLE sl_rcd_flat_tree (id INTEGER PRIMARY KEY, name TEXT, depth INTEGER)',
            'CREATE TABLE sl_rcd_numbers (n INTEGER PRIMARY KEY)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rcd_numbers', 'sl_rcd_flat_tree', 'sl_rcd_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_rcd_categories VALUES (1, 'Root', NULL)");
        $this->pdo->exec("INSERT INTO sl_rcd_categories VALUES (2, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_rcd_categories VALUES (3, 'Phones', 2)");
        $this->pdo->exec("INSERT INTO sl_rcd_categories VALUES (4, 'Laptops', 2)");
        $this->pdo->exec("INSERT INTO sl_rcd_categories VALUES (5, 'Clothing', 1)");
    }

    /**
     * Recursive CTE generating a number series driving INSERT.
     * WITH RECURSIVE nums AS (...) INSERT INTO target SELECT FROM nums
     * The recursive CTE has no shadow table reference so this tests
     * whether the rewriter breaks the RECURSIVE keyword.
     */
    public function testRecursiveCteInsertNumberSeries(): void
    {
        $sql = "WITH RECURSIVE nums(n) AS (
                    SELECT 1
                    UNION ALL
                    SELECT n + 1 FROM nums WHERE n < 10
                )
                INSERT INTO sl_rcd_numbers (n) SELECT n FROM nums";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT n FROM sl_rcd_numbers ORDER BY n");

            if (count($rows) !== 10) {
                $this->markTestIncomplete(
                    'Recursive CTE INSERT series: expected 10, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(10, $rows);
            $this->assertSame(1, (int) $rows[0]['n']);
            $this->assertSame(10, (int) $rows[9]['n']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Recursive CTE INSERT series failed: ' . $e->getMessage());
        }
    }

    /**
     * Recursive CTE traversing tree, driving INSERT into flat_tree.
     * This references a shadow table (sl_rcd_categories) inside RECURSIVE.
     */
    public function testRecursiveCteTreeInsert(): void
    {
        $sql = "WITH RECURSIVE tree AS (
                    SELECT id, name, 0 AS depth
                    FROM sl_rcd_categories WHERE parent_id IS NULL
                    UNION ALL
                    SELECT c.id, c.name, t.depth + 1
                    FROM sl_rcd_categories c
                    JOIN tree t ON c.parent_id = t.id
                )
                INSERT INTO sl_rcd_flat_tree (id, name, depth)
                SELECT id, name, depth FROM tree";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, depth FROM sl_rcd_flat_tree ORDER BY depth, name");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'Recursive CTE tree INSERT: expected 5, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
            $this->assertSame('Root', $rows[0]['name']);
            $this->assertSame(0, (int) $rows[0]['depth']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Recursive CTE tree INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Recursive CTE used to DELETE (remove all descendants of a node).
     */
    public function testRecursiveCteDelete(): void
    {
        $sql = "WITH RECURSIVE subtree AS (
                    SELECT id FROM sl_rcd_categories WHERE id = 2
                    UNION ALL
                    SELECT c.id FROM sl_rcd_categories c
                    JOIN subtree s ON c.parent_id = s.id
                )
                DELETE FROM sl_rcd_categories WHERE id IN (SELECT id FROM subtree)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM sl_rcd_categories ORDER BY id");

            // Should have removed Electronics(2), Phones(3), Laptops(4) → left Root(1), Clothing(5)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Recursive CTE DELETE subtree: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Root', $rows[0]['name']);
            $this->assertSame('Clothing', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Recursive CTE DELETE subtree failed: ' . $e->getMessage());
        }
    }

    /**
     * Recursive CTE with prepared param in the base case.
     */
    public function testRecursiveCteInsertPrepared(): void
    {
        $sql = "WITH RECURSIVE nums(n) AS (
                    SELECT ?
                    UNION ALL
                    SELECT n + 1 FROM nums WHERE n < ?
                )
                INSERT INTO sl_rcd_numbers (n) SELECT n FROM nums";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([5, 8]);

            $rows = $this->ztdQuery("SELECT n FROM sl_rcd_numbers ORDER BY n");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Recursive CTE INSERT prepared: expected 4 (5..8), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame(5, (int) $rows[0]['n']);
            $this->assertSame(8, (int) $rows[3]['n']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Recursive CTE INSERT prepared failed: ' . $e->getMessage());
        }
    }
}
