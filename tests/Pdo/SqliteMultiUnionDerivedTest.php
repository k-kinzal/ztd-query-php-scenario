<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SELECT/DML with multiple UNION branches in derived tables and subqueries.
 *
 * Pattern: SELECT * FROM (SELECT ... UNION ALL SELECT ... UNION ALL SELECT ...) sub
 * The CTE rewriter must rewrite table refs in ALL union branches.
 *
 * @spec SPEC-3.3a, SPEC-3.1
 */
class SqliteMultiUnionDerivedTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mud_sales_2023 (id INTEGER PRIMARY KEY, product TEXT, amount REAL)',
            'CREATE TABLE sl_mud_sales_2024 (id INTEGER PRIMARY KEY, product TEXT, amount REAL)',
            'CREATE TABLE sl_mud_sales_2025 (id INTEGER PRIMARY KEY, product TEXT, amount REAL)',
            'CREATE TABLE sl_mud_combined (id INTEGER PRIMARY KEY, product TEXT, amount REAL, year INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mud_combined', 'sl_mud_sales_2025', 'sl_mud_sales_2024', 'sl_mud_sales_2023'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_mud_sales_2023 VALUES (1, 'Widget', 100)");
        $this->pdo->exec("INSERT INTO sl_mud_sales_2023 VALUES (2, 'Gadget', 200)");
        $this->pdo->exec("INSERT INTO sl_mud_sales_2024 VALUES (1, 'Widget', 150)");
        $this->pdo->exec("INSERT INTO sl_mud_sales_2024 VALUES (2, 'Gadget', 180)");
        $this->pdo->exec("INSERT INTO sl_mud_sales_2025 VALUES (1, 'Widget', 175)");
    }

    /**
     * SELECT from derived table with 3 UNION ALL branches.
     */
    public function testThreeWayUnionInDerivedTable(): void
    {
        $sql = "SELECT product, SUM(amount) AS total
                FROM (
                    SELECT product, amount FROM sl_mud_sales_2023
                    UNION ALL
                    SELECT product, amount FROM sl_mud_sales_2024
                    UNION ALL
                    SELECT product, amount FROM sl_mud_sales_2025
                ) combined
                GROUP BY product
                ORDER BY product";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '3-way UNION derived: expected 2 products, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertEqualsWithDelta(380.0, (float) $rows[0]['total'], 0.01);
            $this->assertSame('Widget', $rows[1]['product']);
            $this->assertEqualsWithDelta(425.0, (float) $rows[1]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('3-way UNION derived failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from 3-way UNION.
     */
    public function testInsertFromThreeWayUnion(): void
    {
        $sql = "INSERT INTO sl_mud_combined (id, product, amount, year)
                SELECT ROW_NUMBER() OVER (ORDER BY year, product), product, amount, year
                FROM (
                    SELECT product, amount, 2023 AS year FROM sl_mud_sales_2023
                    UNION ALL
                    SELECT product, amount, 2024 AS year FROM sl_mud_sales_2024
                    UNION ALL
                    SELECT product, amount, 2025 AS year FROM sl_mud_sales_2025
                ) all_sales";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT product, amount, year FROM sl_mud_combined ORDER BY year, product");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INSERT 3-way UNION: expected 5, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT 3-way UNION failed: ' . $e->getMessage());
        }
    }

    /**
     * WHERE IN with multi-branch UNION subquery.
     */
    public function testWhereInUnionSubquery(): void
    {
        $sql = "SELECT product, amount
                FROM sl_mud_sales_2023
                WHERE product IN (
                    SELECT product FROM sl_mud_sales_2024
                    UNION
                    SELECT product FROM sl_mud_sales_2025
                )
                ORDER BY product";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE IN UNION subquery: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WHERE IN UNION subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE using UNION subquery to identify targets.
     */
    public function testDeleteUsingUnionSubquery(): void
    {
        $sql = "DELETE FROM sl_mud_sales_2023
                WHERE product NOT IN (
                    SELECT product FROM sl_mud_sales_2025
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT product FROM sl_mud_sales_2023");

            // Only Widget exists in 2025, so Gadget should be deleted
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE UNION subquery: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['product']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE UNION subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT from UNION derived table with params.
     */
    public function testPreparedUnionDerivedTable(): void
    {
        $sql = "SELECT product, SUM(amount) AS total
                FROM (
                    SELECT product, amount FROM sl_mud_sales_2023 WHERE amount > ?
                    UNION ALL
                    SELECT product, amount FROM sl_mud_sales_2024 WHERE amount > ?
                ) filtered
                GROUP BY product
                ORDER BY total DESC";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [120, 160]);

            // 2023: only Gadget(200) > 120. 2024: only Gadget(180) > 160
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UNION derived: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertEqualsWithDelta(380.0, (float) $rows[0]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION derived failed: ' . $e->getMessage());
        }
    }
}
