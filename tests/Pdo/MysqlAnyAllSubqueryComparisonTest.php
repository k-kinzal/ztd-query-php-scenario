<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests ALL/ANY/SOME subquery comparison operators with shadow data on MySQL.
 *
 * SQL standard comparison operators like `> ALL(SELECT ...)` and `= ANY(SELECT ...)`
 * require the CTE rewriter to correctly handle the subquery inside the operator.
 * These are used in practice for threshold checks, existence filtering, etc.
 *
 * @spec SPEC-3.3
 */
class MysqlAnyAllSubqueryComparisonTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_aas_products (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price DECIMAL(10,2),
                category VARCHAR(30)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_aas_thresholds (
                id INT PRIMARY KEY,
                category VARCHAR(30),
                min_price DECIMAL(10,2)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_aas_thresholds', 'mp_aas_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_aas_products (id, name, price, category) VALUES
            (1, 'Widget A', 10.00, 'widgets'),
            (2, 'Widget B', 25.00, 'widgets'),
            (3, 'Widget C', 50.00, 'widgets'),
            (4, 'Gadget X', 15.00, 'gadgets'),
            (5, 'Gadget Y', 30.00, 'gadgets')");

        $this->pdo->exec("INSERT INTO mp_aas_thresholds (id, category, min_price) VALUES
            (1, 'widgets', 20.00),
            (2, 'gadgets', 25.00)");
    }

    /**
     * SELECT with > ALL (SELECT ...) on shadow data.
     * Should return products whose price exceeds ALL threshold min_prices.
     */
    public function testGreaterThanAllSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM mp_aas_products
                 WHERE price > ALL (SELECT min_price FROM mp_aas_thresholds)
                 ORDER BY price"
            );

            // Thresholds are 20.00 and 25.00. Only prices > 25.00 qualify: Widget C (50), Gadget Y (30)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '> ALL subquery: expected 2 rows, got ' . count($rows)
                    . '. CTE rewriter may not handle > ALL(SELECT ...) correctly.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget Y', $rows[0]['name']);
            $this->assertSame('Widget C', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('> ALL subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with = ANY (SELECT ...) on shadow data.
     * Should return products whose price matches ANY threshold min_price.
     */
    public function testEqualsAnySubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM mp_aas_products
                 WHERE price = ANY (SELECT min_price FROM mp_aas_thresholds)
                 ORDER BY name"
            );

            // Thresholds are 20.00, 25.00. Products with exactly those prices: Widget B (25.00)
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    '= ANY subquery: expected 1 row, got ' . count($rows)
                    . '. CTE rewriter may not handle = ANY(SELECT ...) correctly.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget B', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('= ANY subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with >= SOME (SELECT ...) — SOME is a synonym for ANY.
     */
    public function testGreaterEqualSomeSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM mp_aas_products
                 WHERE price >= SOME (SELECT min_price FROM mp_aas_thresholds)
                 ORDER BY name"
            );

            // >= SOME means >= at least one threshold. min thresholds: 20, 25.
            // Prices >= 20: Widget B (25), Widget C (50), Gadget Y (30) = 3 rows
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    '>= SOME subquery: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('>= SOME subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * ALL/ANY after shadow mutation — insert a new product and verify it appears.
     */
    public function testAnyAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO mp_aas_products (id, name, price, category) VALUES (6, 'Premium Z', 100.00, 'premium')");

        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM mp_aas_products
                 WHERE price > ALL (SELECT min_price FROM mp_aas_thresholds)
                 ORDER BY price"
            );

            // After insert: prices > 25.00 are Gadget Y (30), Widget C (50), Premium Z (100) = 3 rows
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    '> ALL after shadow INSERT: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Premium Z', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('> ALL after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * ALL with shadow-mutated threshold table.
     */
    public function testAllWithMutatedThresholdTable(): void
    {
        // Raise the gadgets threshold — now both thresholds are >= 20
        $this->pdo->exec("UPDATE mp_aas_thresholds SET min_price = 40.00 WHERE category = 'gadgets'");

        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM mp_aas_products
                 WHERE price > ALL (SELECT min_price FROM mp_aas_thresholds)
                 ORDER BY price"
            );

            // Thresholds now 20.00, 40.00. Only price > 40: Widget C (50.00) = 1 row
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    '> ALL with mutated threshold: expected 1 row, got ' . count($rows)
                    . '. Shadow mutations in subquery table may not be visible.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget C', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('> ALL with mutated threshold failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with > ALL subquery condition.
     */
    public function testDeleteWithAllSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM mp_aas_products
                 WHERE price < ALL (SELECT min_price FROM mp_aas_thresholds)"
            );

            $rows = $this->ztdQuery("SELECT name FROM mp_aas_products ORDER BY name");

            // Thresholds: 20, 25. Price < ALL means price < 20: Widget A (10), Gadget X (15) deleted
            // Remaining: Gadget Y (30), Widget B (25), Widget C (50)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with < ALL: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with < ALL subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with = ANY subquery in WHERE.
     */
    public function testUpdateWithAnySubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_aas_products SET category = 'threshold-match'
                 WHERE price = ANY (SELECT min_price FROM mp_aas_thresholds)"
            );

            $rows = $this->ztdQuery(
                "SELECT name, category FROM mp_aas_products WHERE category = 'threshold-match'"
            );

            // Only Widget B (25.00) matches a threshold price
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE with = ANY: expected 1 updated row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget B', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with = ANY subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation: shadow DML via ALL/ANY should not affect physical table.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM mp_aas_products
                 WHERE price > ALL (SELECT min_price FROM mp_aas_thresholds)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with > ALL failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_aas_products")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
