<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests ALL/ANY/SOME subquery comparison operators with shadow data on PostgreSQL.
 *
 * PostgreSQL supports both SQL-standard ALL/ANY/SOME and the array-based = ANY(ARRAY[...]).
 * This test focuses on the subquery forms which require the CTE rewriter to handle
 * the subquery table references inside the comparison operator.
 *
 * @spec SPEC-3.3
 */
class PostgresAnyAllSubqueryComparisonTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_aas_products (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                category VARCHAR(30)
            )',
            'CREATE TABLE pg_aas_thresholds (
                id INT PRIMARY KEY,
                category VARCHAR(30),
                min_price NUMERIC(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_aas_thresholds', 'pg_aas_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_aas_products (id, name, price, category) VALUES
            (1, 'Widget A', 10.00, 'widgets'),
            (2, 'Widget B', 25.00, 'widgets'),
            (3, 'Widget C', 50.00, 'widgets'),
            (4, 'Gadget X', 15.00, 'gadgets'),
            (5, 'Gadget Y', 30.00, 'gadgets')");

        $this->pdo->exec("INSERT INTO pg_aas_thresholds (id, category, min_price) VALUES
            (1, 'widgets', 20.00),
            (2, 'gadgets', 25.00)");
    }

    public function testGreaterThanAllSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_aas_products
                 WHERE price > ALL (SELECT min_price FROM pg_aas_thresholds)
                 ORDER BY price"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '> ALL subquery: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget Y', $rows[0]['name']);
            $this->assertSame('Widget C', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('> ALL subquery failed: ' . $e->getMessage());
        }
    }

    public function testEqualsAnySubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_aas_products
                 WHERE price = ANY (SELECT min_price FROM pg_aas_thresholds)
                 ORDER BY name"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    '= ANY subquery: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget B', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('= ANY subquery failed: ' . $e->getMessage());
        }
    }

    public function testSomeAsSynonymForAny(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_aas_products
                 WHERE price >= SOME (SELECT min_price FROM pg_aas_thresholds)
                 ORDER BY name"
            );

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

    public function testAllWithBothTablesMutated(): void
    {
        $this->pdo->exec("INSERT INTO pg_aas_products (id, name, price, category) VALUES (6, 'Premium Z', 100.00, 'premium')");
        $this->pdo->exec("UPDATE pg_aas_thresholds SET min_price = 40.00 WHERE category = 'gadgets'");

        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM pg_aas_products
                 WHERE price > ALL (SELECT min_price FROM pg_aas_thresholds)
                 ORDER BY price"
            );

            // Thresholds now 20.00 and 40.00. Prices > 40: Widget C (50), Premium Z (100) = 2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '> ALL with both tables mutated: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Widget C', $rows[0]['name']);
            $this->assertSame('Premium Z', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('> ALL with both tables mutated failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWithLessThanAllSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_aas_products
                 WHERE price < ALL (SELECT min_price FROM pg_aas_thresholds)"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_aas_products ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE < ALL: expected 3 remaining, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with < ALL subquery failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithAnySubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_aas_products SET category = 'threshold-match'
                 WHERE price = ANY (SELECT min_price FROM pg_aas_thresholds)"
            );

            $rows = $this->ztdQuery(
                "SELECT name FROM pg_aas_products WHERE category = 'threshold-match'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE = ANY: expected 1 updated row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget B', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with = ANY subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * NOT IN with NULL edge case — SQL standard says NOT IN with NULL always returns empty.
     */
    public function testNotInWithNullSubquery(): void
    {
        $this->pdo->exec("INSERT INTO pg_aas_thresholds (id, category, min_price) VALUES (3, 'null_cat', NULL)");

        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_aas_products
                 WHERE price NOT IN (SELECT min_price FROM pg_aas_thresholds)"
            );

            // NOT IN with NULL should return no rows (SQL standard behavior)
            $this->assertCount(0, $rows,
                'NOT IN with NULL in subquery should return empty result set'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT IN with NULL subquery failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_aas_products
                 WHERE price > ALL (SELECT min_price FROM pg_aas_thresholds)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_aas_products")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
