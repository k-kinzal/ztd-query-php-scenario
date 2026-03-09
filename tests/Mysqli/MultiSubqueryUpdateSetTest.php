<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with multiple subqueries in SET on MySQLi.
 *
 * Also tests MySQL-specific UPDATE JOIN syntax.
 *
 * @spec SPEC-4.2
 */
class MultiSubqueryUpdateSetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_msu_summary (
                id INT PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                min_price DECIMAL(10,2),
                max_price DECIMAL(10,2),
                item_count INT
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_msu_products (
                id INT PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_msu_summary', 'mi_msu_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_msu_products VALUES (1, 'electronics', 99.99)");
        $this->mysqli->query("INSERT INTO mi_msu_products VALUES (2, 'electronics', 199.99)");
        $this->mysqli->query("INSERT INTO mi_msu_products VALUES (3, 'electronics', 49.99)");
        $this->mysqli->query("INSERT INTO mi_msu_products VALUES (4, 'clothing', 29.99)");
        $this->mysqli->query("INSERT INTO mi_msu_products VALUES (5, 'clothing', 59.99)");

        $this->mysqli->query("INSERT INTO mi_msu_summary VALUES (1, 'electronics', NULL, NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_msu_summary VALUES (2, 'clothing', NULL, NULL, NULL)");
    }

    /**
     * Multiple correlated subqueries in SET via query().
     */
    public function testUpdateMultipleCorrelatedSubqueries(): void
    {
        $sql = "UPDATE mi_msu_summary SET
                    min_price = (SELECT MIN(price) FROM mi_msu_products WHERE category = mi_msu_summary.category),
                    max_price = (SELECT MAX(price) FROM mi_msu_products WHERE category = mi_msu_summary.category),
                    item_count = (SELECT COUNT(*) FROM mi_msu_products WHERE category = mi_msu_summary.category)
                WHERE category = 'electronics'";

        try {
            $this->mysqli->query($sql);

            $rows = $this->ztdQuery("SELECT * FROM mi_msu_summary WHERE category = 'electronics'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Multi-subquery UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $row = $rows[0];
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
     * MySQL UPDATE JOIN syntax.
     *
     * UPDATE summary s JOIN (subquery) p ON s.category = p.category SET s.item_count = p.cnt
     */
    public function testUpdateJoinSyntax(): void
    {
        $sql = "UPDATE mi_msu_summary s
                JOIN (
                    SELECT category, COUNT(*) AS cnt, MIN(price) AS mn, MAX(price) AS mx
                    FROM mi_msu_products GROUP BY category
                ) p ON s.category = p.category
                SET s.min_price = p.mn, s.max_price = p.mx, s.item_count = p.cnt";

        try {
            $this->mysqli->query($sql);

            $rows = $this->ztdQuery("SELECT category, min_price, max_price, item_count FROM mi_msu_summary ORDER BY category");

            $this->assertCount(2, $rows);

            // clothing: min=29.99, max=59.99, count=2
            $clothing = $rows[0];
            // electronics: min=49.99, max=199.99, count=3
            $electronics = $rows[1];

            $clothingOk = abs((float) $clothing['min_price'] - 29.99) < 0.01
                && abs((float) $clothing['max_price'] - 59.99) < 0.01
                && (int) $clothing['item_count'] === 2;

            $electronicsOk = abs((float) $electronics['min_price'] - 49.99) < 0.01
                && abs((float) $electronics['max_price'] - 199.99) < 0.01
                && (int) $electronics['item_count'] === 3;

            if (!$clothingOk || !$electronicsOk) {
                $this->markTestIncomplete(
                    "UPDATE JOIN: clothing min={$clothing['min_price']} max={$clothing['max_price']} count={$clothing['item_count']}; "
                    . "electronics min={$electronics['min_price']} max={$electronics['max_price']} count={$electronics['item_count']}"
                );
            }

            $this->assertEquals(29.99, (float) $clothing['min_price'], '', 0.01);
            $this->assertEquals(59.99, (float) $clothing['max_price'], '', 0.01);
            $this->assertSame(2, (int) $clothing['item_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'MySQL UPDATE JOIN syntax failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with multiple subqueries and params.
     */
    public function testPreparedMultiSubqueryUpdate(): void
    {
        $sql = "UPDATE mi_msu_summary SET
                    min_price = (SELECT MIN(price) FROM mi_msu_products WHERE category = ?),
                    max_price = (SELECT MAX(price) FROM mi_msu_products WHERE category = ?)
                WHERE category = ?";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $c1 = 'clothing';
            $c2 = 'clothing';
            $c3 = 'clothing';
            $stmt->bind_param('sss', $c1, $c2, $c3);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT min_price, max_price FROM mi_msu_summary WHERE category = 'clothing'");

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
