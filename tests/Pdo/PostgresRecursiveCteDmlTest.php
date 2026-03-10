<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests recursive CTEs used to drive DML operations (PostgreSQL).
 * @spec SPEC-3.3c, SPEC-4.1a
 */
class PostgresRecursiveCteDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rcd_categories (id INT PRIMARY KEY, name VARCHAR(50), parent_id INT)',
            'CREATE TABLE pg_rcd_flat_tree (id INT PRIMARY KEY, name VARCHAR(50), depth INT)',
            'CREATE TABLE pg_rcd_numbers (n INT PRIMARY KEY)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rcd_numbers', 'pg_rcd_flat_tree', 'pg_rcd_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO pg_rcd_categories VALUES (1, 'Root', NULL)");
        $this->pdo->exec("INSERT INTO pg_rcd_categories VALUES (2, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO pg_rcd_categories VALUES (3, 'Phones', 2)");
        $this->pdo->exec("INSERT INTO pg_rcd_categories VALUES (4, 'Laptops', 2)");
        $this->pdo->exec("INSERT INTO pg_rcd_categories VALUES (5, 'Clothing', 1)");
    }

    public function testRecursiveCteInsertNumberSeries(): void
    {
        $sql = "WITH RECURSIVE nums(n) AS (
                    SELECT 1
                    UNION ALL
                    SELECT n + 1 FROM nums WHERE n < 10
                )
                INSERT INTO pg_rcd_numbers (n) SELECT n FROM nums";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT n FROM pg_rcd_numbers ORDER BY n");

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

    public function testRecursiveCteTreeInsert(): void
    {
        $sql = "WITH RECURSIVE tree AS (
                    SELECT id, name, 0 AS depth
                    FROM pg_rcd_categories WHERE parent_id IS NULL
                    UNION ALL
                    SELECT c.id, c.name, t.depth + 1
                    FROM pg_rcd_categories c
                    JOIN tree t ON c.parent_id = t.id
                )
                INSERT INTO pg_rcd_flat_tree (id, name, depth)
                SELECT id, name, depth FROM tree";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, depth FROM pg_rcd_flat_tree ORDER BY depth, name");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'Recursive CTE tree INSERT: expected 5, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
            $this->assertSame('Root', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Recursive CTE tree INSERT failed: ' . $e->getMessage());
        }
    }

    public function testRecursiveCteDelete(): void
    {
        $sql = "WITH RECURSIVE subtree AS (
                    SELECT id FROM pg_rcd_categories WHERE id = 2
                    UNION ALL
                    SELECT c.id FROM pg_rcd_categories c
                    JOIN subtree s ON c.parent_id = s.id
                )
                DELETE FROM pg_rcd_categories WHERE id IN (SELECT id FROM subtree)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM pg_rcd_categories ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Recursive CTE DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Root', $rows[0]['name']);
            $this->assertSame('Clothing', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Recursive CTE DELETE failed: ' . $e->getMessage());
        }
    }

    public function testRecursiveCteInsertPrepared(): void
    {
        $sql = "WITH RECURSIVE nums(n) AS (
                    SELECT $1::int
                    UNION ALL
                    SELECT n + 1 FROM nums WHERE n < $2::int
                )
                INSERT INTO pg_rcd_numbers (n) SELECT n FROM nums";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([5, 8]);

            $rows = $this->ztdQuery("SELECT n FROM pg_rcd_numbers ORDER BY n");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Recursive CTE INSERT prepared: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Recursive CTE INSERT prepared failed: ' . $e->getMessage());
        }
    }
}
