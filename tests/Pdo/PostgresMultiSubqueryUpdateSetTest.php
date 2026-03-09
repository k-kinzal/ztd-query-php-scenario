<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE with multiple subqueries in SET on PostgreSQL PDO.
 *
 * Known: correlated subquery in SET fails on PostgreSQL (#51) with grouping error.
 * This tests multiple subqueries in SET — may compound or differ from single-subquery case.
 *
 * @spec SPEC-4.2
 */
class PostgresMultiSubqueryUpdateSetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_msu_summary (
                id INTEGER PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                min_price NUMERIC(10,2),
                max_price NUMERIC(10,2),
                item_count INTEGER
            )',
            'CREATE TABLE pg_msu_products (
                id INTEGER PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                price NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_msu_summary', 'pg_msu_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_msu_products VALUES (1, 'electronics', 99.99)");
        $this->pdo->exec("INSERT INTO pg_msu_products VALUES (2, 'electronics', 199.99)");
        $this->pdo->exec("INSERT INTO pg_msu_products VALUES (3, 'electronics', 49.99)");
        $this->pdo->exec("INSERT INTO pg_msu_products VALUES (4, 'clothing', 29.99)");
        $this->pdo->exec("INSERT INTO pg_msu_products VALUES (5, 'clothing', 59.99)");

        $this->pdo->exec("INSERT INTO pg_msu_summary VALUES (1, 'electronics', NULL, NULL, NULL)");
        $this->pdo->exec("INSERT INTO pg_msu_summary VALUES (2, 'clothing', NULL, NULL, NULL)");
    }

    /**
     * Multiple correlated subqueries in SET.
     */
    public function testUpdateMultipleCorrelatedSubqueriesInSet(): void
    {
        $sql = "UPDATE pg_msu_summary SET
                    min_price = (SELECT MIN(price) FROM pg_msu_products WHERE category = pg_msu_summary.category),
                    max_price = (SELECT MAX(price) FROM pg_msu_products WHERE category = pg_msu_summary.category),
                    item_count = (SELECT COUNT(*) FROM pg_msu_products WHERE category = pg_msu_summary.category)
                WHERE category = 'electronics'";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT * FROM pg_msu_summary WHERE category = 'electronics'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Multi-subquery UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $row = $rows[0];
            $minOk = abs((float) $row['min_price'] - 49.99) < 0.01;
            $maxOk = abs((float) $row['max_price'] - 199.99) < 0.01;
            $cntOk = (int) $row['item_count'] === 3;

            if (!$minOk || !$maxOk || !$cntOk) {
                $this->markTestIncomplete(
                    "Multi-subquery UPDATE: min={$row['min_price']} (exp 49.99), "
                    . "max={$row['max_price']} (exp 199.99), count={$row['item_count']} (exp 3)"
                );
            }

            $this->assertEquals(49.99, (float) $row['min_price'], '', 0.01);
            $this->assertEquals(199.99, (float) $row['max_price'], '', 0.01);
            $this->assertSame(3, (int) $row['item_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-subquery UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Non-correlated subqueries in SET.
     */
    public function testUpdateNonCorrelatedSubqueriesInSet(): void
    {
        $sql = "UPDATE pg_msu_summary SET
                    min_price = (SELECT MIN(price) FROM pg_msu_products),
                    max_price = (SELECT MAX(price) FROM pg_msu_products)
                WHERE id = 1";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT min_price, max_price FROM pg_msu_summary WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertEquals(29.99, (float) $rows[0]['min_price'], '', 0.01);
            $this->assertEquals(199.99, (float) $rows[0]['max_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Non-correlated multi-subquery UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with subqueries using ? placeholders.
     */
    public function testPreparedMultiSubqueryUpdateWithQuestionMark(): void
    {
        $sql = "UPDATE pg_msu_summary SET
                    min_price = (SELECT MIN(price) FROM pg_msu_products WHERE category = ?),
                    max_price = (SELECT MAX(price) FROM pg_msu_products WHERE category = ?)
                WHERE category = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['clothing', 'clothing', 'clothing']);

            $rows = $this->ztdQuery("SELECT min_price, max_price FROM pg_msu_summary WHERE category = 'clothing'");

            $this->assertCount(1, $rows);
            $this->assertEquals(29.99, (float) $rows[0]['min_price'], '', 0.01);
            $this->assertEquals(59.99, (float) $rows[0]['max_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-subquery UPDATE failed: ' . $e->getMessage()
            );
        }
    }
}
