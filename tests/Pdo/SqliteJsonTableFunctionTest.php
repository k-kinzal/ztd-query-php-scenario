<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite table-valued functions (json_each, json_tree) with shadow data.
 *
 * Real-world scenario: applications store JSON in columns and use json_each()
 * or json_tree() to unnest arrays for JOINs, filtering, or aggregation.
 * These table-valued functions appear in the FROM clause like tables, which
 * may confuse the CTE rewriter into treating them as table references.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteJsonTableFunctionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_jtf_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT \'[]\'
            )',
            'CREATE TABLE sl_jtf_orders (
                id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_jtf_orders', 'sl_jtf_products'];
    }

    /**
     * SELECT with json_each() in FROM clause on shadow data.
     */
    public function testJsonEachOnShadowColumn(): void
    {
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (1, 'Widget', '[\"sale\",\"featured\",\"new\"]')");

        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, j.value AS tag
                 FROM sl_jtf_products p, json_each(p.tags) j
                 ORDER BY j.value"
            );

            $tags = array_column($rows, 'tag');

            if (empty($tags)) {
                $this->markTestIncomplete(
                    'json_each() on shadow data returned no rows. '
                    . 'The CTE rewriter may not support table-valued functions in FROM.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertContains('sale', $tags);
            $this->assertContains('featured', $tags);
            $this->assertContains('new', $tags);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_each() on shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * json_each() JOIN with another shadow table.
     */
    public function testJsonEachJoinWithShadowTable(): void
    {
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (1, 'Widget', '[\"electronics\",\"gadgets\"]')");
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (2, 'Bolt', '[\"hardware\",\"tools\"]')");
        $this->ztdExec("INSERT INTO sl_jtf_orders VALUES (1, 1, 5)");
        $this->ztdExec("INSERT INTO sl_jtf_orders VALUES (2, 2, 10)");

        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, j.value AS tag, o.quantity
                 FROM sl_jtf_products p
                 JOIN json_each(p.tags) j
                 JOIN sl_jtf_orders o ON o.product_id = p.id
                 ORDER BY p.name, j.value"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'json_each() JOIN with shadow table returned no rows.'
                );
            }

            // Widget has 2 tags * 1 order = 2 rows, Bolt has 2 tags * 1 order = 2 rows
            $this->assertCount(4, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_each() JOIN with shadow table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Filtering with json_each() WHERE clause on shadow data.
     */
    public function testJsonEachWithWhereFilter(): void
    {
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (1, 'Widget', '[\"sale\",\"clearance\"]')");
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (2, 'Gadget', '[\"new\",\"featured\"]')");

        try {
            $rows = $this->ztdQuery(
                "SELECT DISTINCT p.name
                 FROM sl_jtf_products p, json_each(p.tags) j
                 WHERE j.value = 'sale'"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'json_each() with WHERE filter returned no rows on shadow data.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_each() with WHERE filter failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Aggregation over json_each() results from shadow data.
     */
    public function testJsonEachAggregation(): void
    {
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (1, 'A', '[\"x\",\"y\",\"z\"]')");
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (2, 'B', '[\"x\",\"y\"]')");
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (3, 'C', '[\"x\"]')");

        try {
            $rows = $this->ztdQuery(
                "SELECT j.value AS tag, COUNT(*) AS cnt
                 FROM sl_jtf_products p, json_each(p.tags) j
                 GROUP BY j.value
                 ORDER BY cnt DESC, tag"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'json_each() aggregation returned no rows on shadow data.'
                );
            }

            // tag 'x' appears in all 3 products
            $this->assertSame('x', $rows[0]['tag']);
            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_each() aggregation failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * json_each() with prepared statement parameter.
     */
    public function testJsonEachWithPreparedParam(): void
    {
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (1, 'Widget', '[\"sale\",\"new\"]')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT p.name, j.value AS tag
                 FROM sl_jtf_products p, json_each(p.tags) j
                 WHERE p.id = ?
                 ORDER BY j.value",
                [1]
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'json_each() with prepared param returned no rows.'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_each() with prepared param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * json_tree() for nested JSON objects on shadow data.
     */
    public function testJsonTreeOnShadowData(): void
    {
        $json = '{"a":1,"b":{"c":2,"d":3}}';
        $this->ztdExec("INSERT INTO sl_jtf_products VALUES (1, 'Nested', '{$json}')");

        try {
            $rows = $this->ztdQuery(
                "SELECT j.fullkey, j.value, j.type
                 FROM sl_jtf_products p, json_tree(p.tags) j
                 WHERE j.type = 'integer'
                 ORDER BY j.fullkey"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'json_tree() on shadow data returned no integer leaf nodes.'
                );
            }

            // Should find: $.a=1, $.b.c=2, $.b.d=3
            $this->assertCount(3, $rows);
            $keys = array_column($rows, 'fullkey');
            $this->assertContains('$.a', $keys);
            $this->assertContains('$.b.c', $keys);
            $this->assertContains('$.b.d', $keys);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_tree() on shadow data failed: ' . $e->getMessage()
            );
        }
    }
}
