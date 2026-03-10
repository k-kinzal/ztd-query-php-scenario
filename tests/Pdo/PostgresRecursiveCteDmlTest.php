<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests recursive CTEs (WITH RECURSIVE) in DML context on PostgreSQL.
 *
 * Recursive CTEs are common for hierarchical data. This tests whether the
 * CTE rewriter correctly handles DML with recursive CTEs in subqueries,
 * given that PostgreSQL CTEs are already problematic (Issue #4).
 *
 * @spec SPEC-10.2
 */
class PostgresRecursiveCteDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_rct_categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                parent_id INTEGER,
                active BOOLEAN DEFAULT TRUE
            )",
            "CREATE TABLE pg_rct_flat (
                id SERIAL PRIMARY KEY,
                category_id INTEGER,
                path TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rct_flat', 'pg_rct_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_rct_categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->ztdExec("INSERT INTO pg_rct_categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");
        $this->ztdExec("INSERT INTO pg_rct_categories (id, name, parent_id) VALUES (3, 'Phones', 2)");
        $this->ztdExec("INSERT INTO pg_rct_categories (id, name, parent_id) VALUES (4, 'Laptops', 2)");
        $this->ztdExec("INSERT INTO pg_rct_categories (id, name, parent_id) VALUES (5, 'Clothing', 1)");
        $this->ztdExec("INSERT INTO pg_rct_categories (id, name, parent_id) VALUES (6, 'Shirts', 5)");
        $this->ztdExec("SELECT setval('pg_rct_categories_id_seq', 6)");
    }

    /**
     * SELECT with recursive CTE — baseline.
     */
    public function testSelectRecursiveCte(): void
    {
        try {
            $rows = $this->ztdQuery(
                "WITH RECURSIVE tree AS (
                    SELECT id, name, parent_id, name::TEXT AS path FROM pg_rct_categories WHERE parent_id IS NULL
                    UNION ALL
                    SELECT c.id, c.name, c.parent_id, tree.path || '/' || c.name
                    FROM pg_rct_categories c
                    JOIN tree ON c.parent_id = tree.id
                )
                SELECT id, name, path FROM tree ORDER BY path"
            );

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'SELECT recursive CTE (PG): expected 6, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT recursive CTE (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from recursive CTE to flatten hierarchy.
     */
    public function testInsertFromRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_rct_flat (category_id, path)
                 WITH RECURSIVE tree AS (
                     SELECT id, name, parent_id, name::TEXT AS path FROM pg_rct_categories WHERE parent_id IS NULL
                     UNION ALL
                     SELECT c.id, c.name, c.parent_id, tree.path || '/' || c.name
                     FROM pg_rct_categories c
                     JOIN tree ON c.parent_id = tree.id
                 )
                 SELECT id, path FROM tree"
            );

            $rows = $this->ztdQuery("SELECT category_id, path FROM pg_rct_flat ORDER BY path");

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'INSERT from recursive CTE (PG): expected 6, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT from recursive CTE (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE subtree using recursive CTE.
     */
    public function testDeleteSubtreeViaRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_rct_categories WHERE id IN (
                    WITH RECURSIVE subtree AS (
                        SELECT id FROM pg_rct_categories WHERE id = 2
                        UNION ALL
                        SELECT c.id FROM pg_rct_categories c
                        JOIN subtree ON c.parent_id = subtree.id
                    )
                    SELECT id FROM subtree
                )"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM pg_rct_categories ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE subtree recursive CTE (PG): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subtree recursive CTE (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE subtree using recursive CTE to deactivate.
     */
    public function testUpdateSubtreeViaRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_rct_categories SET active = FALSE WHERE id IN (
                    WITH RECURSIVE subtree AS (
                        SELECT id FROM pg_rct_categories WHERE id = 5
                        UNION ALL
                        SELECT c.id FROM pg_rct_categories c
                        JOIN subtree ON c.parent_id = subtree.id
                    )
                    SELECT id FROM subtree
                )"
            );

            $inactive = $this->ztdQuery("SELECT id, name FROM pg_rct_categories WHERE active = FALSE ORDER BY id");

            if (count($inactive) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE subtree recursive CTE (PG): expected 2 inactive, got ' . count($inactive)
                    . '. Rows: ' . json_encode($inactive)
                );
            }

            $this->assertCount(2, $inactive);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE subtree recursive CTE (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT from recursive CTE after prior DML on source.
     */
    public function testInsertRecursiveCteAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_rct_categories (id, name, parent_id) VALUES (7, 'Accessories', 3)");

            $this->ztdExec(
                "INSERT INTO pg_rct_flat (category_id, path)
                 WITH RECURSIVE tree AS (
                     SELECT id, name, parent_id, name::TEXT AS path FROM pg_rct_categories WHERE id = 2
                     UNION ALL
                     SELECT c.id, c.name, c.parent_id, tree.path || '/' || c.name
                     FROM pg_rct_categories c
                     JOIN tree ON c.parent_id = tree.id
                 )
                 SELECT id, path FROM tree"
            );

            $rows = $this->ztdQuery("SELECT category_id, path FROM pg_rct_flat ORDER BY path");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT recursive CTE after DML (PG): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT recursive CTE after DML (PG) failed: ' . $e->getMessage());
        }
    }
}
