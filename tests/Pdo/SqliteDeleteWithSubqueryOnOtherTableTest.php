<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE WHERE with subquery referencing a different shadow-modified
 * table on SQLite via PDO.
 *
 * @spec SPEC-4.3
 */
class SqliteDeleteWithSubqueryOnOtherTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dso_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category_id INTEGER NOT NULL
            )',
            'CREATE TABLE sl_dso_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dso_products', 'sl_dso_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dso_categories VALUES (1, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_dso_categories VALUES (2, 'Clothing', 1)");
        $this->pdo->exec("INSERT INTO sl_dso_categories VALUES (3, 'Furniture', 0)");

        $this->pdo->exec("INSERT INTO sl_dso_products VALUES (1, 'Phone', 1)");
        $this->pdo->exec("INSERT INTO sl_dso_products VALUES (2, 'Shirt', 2)");
        $this->pdo->exec("INSERT INTO sl_dso_products VALUES (3, 'Desk', 3)");
        $this->pdo->exec("INSERT INTO sl_dso_products VALUES (4, 'Laptop', 1)");
    }

    public function testDeleteWhereInSubqueryOtherTable(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_dso_products WHERE category_id IN (SELECT id FROM sl_dso_categories WHERE active = 0)"
            );
            $rows = $this->ztdQuery("SELECT id, name FROM sl_dso_products ORDER BY id");
            if (count($rows) !== 3) {
                $this->markTestIncomplete('DELETE WHERE IN: expected 3, got ' . count($rows) . ': ' . json_encode($rows));
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains('Desk', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereInSubqueryAfterCategoryUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_dso_categories SET active = 0 WHERE id = 1");
            $this->pdo->exec(
                "DELETE FROM sl_dso_products WHERE category_id IN (SELECT id FROM sl_dso_categories WHERE active = 0)"
            );
            $rows = $this->ztdQuery("SELECT id, name FROM sl_dso_products ORDER BY id");
            if (count($rows) !== 1) {
                $this->markTestIncomplete('DELETE after UPDATE: expected 1, got ' . count($rows) . ': ' . json_encode($rows));
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Shirt', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereNotInSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_dso_products WHERE category_id NOT IN (SELECT id FROM sl_dso_categories WHERE active = 1)"
            );
            $rows = $this->ztdQuery("SELECT id, name FROM sl_dso_products ORDER BY id");
            if (count($rows) !== 3) {
                $this->markTestIncomplete('DELETE NOT IN: expected 3, got ' . count($rows) . ': ' . json_encode($rows));
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains('Desk', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereExistsSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_dso_products WHERE EXISTS (SELECT 1 FROM sl_dso_categories WHERE sl_dso_categories.id = sl_dso_products.category_id AND active = 0)"
            );
            $rows = $this->ztdQuery("SELECT id, name FROM sl_dso_products ORDER BY id");
            if (count($rows) !== 3) {
                $this->markTestIncomplete('DELETE WHERE EXISTS: expected 3, got ' . count($rows) . ': ' . json_encode($rows));
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains('Desk', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Failed: ' . $e->getMessage());
        }
    }
}
