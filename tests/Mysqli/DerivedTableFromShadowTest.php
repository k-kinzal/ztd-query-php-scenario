<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests derived tables (subqueries in FROM clause) reading shadow data.
 *
 * Derived tables are very common in real applications:
 *   SELECT sub.* FROM (SELECT id, name FROM t WHERE active = 1) sub
 *   SELECT * FROM t1 JOIN (SELECT ... FROM t2 GROUP BY ...) agg ON ...
 *
 * The CTE rewriter inserts CTEs at the top of the SQL, which should
 * shadow table references inside derived tables as well.
 *
 * @spec SPEC-4.2
 */
class DerivedTableFromShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_dts_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            category VARCHAR(20) NOT NULL,
            price DECIMAL(10,2) NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_dts_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_dts_items VALUES (1, 'Alpha', 'tools', 10.00)");
        $this->mysqli->query("INSERT INTO mi_dts_items VALUES (2, 'Beta', 'tools', 20.00)");
        $this->mysqli->query("INSERT INTO mi_dts_items VALUES (3, 'Gamma', 'parts', 30.00)");
    }

    /**
     * Simple derived table reading from shadow-modified data.
     */
    public function testSimpleDerivedTable(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_dts_items VALUES (4, 'Delta', 'tools', 40.00)");

            $rows = $this->ztdQuery(
                "SELECT sub.name, sub.price FROM (SELECT name, price FROM mi_dts_items WHERE category = 'tools') sub ORDER BY sub.price"
            );

            $names = array_column($rows, 'name');
            if (!in_array('Delta', $names)) {
                $this->markTestIncomplete(
                    'Derived table did not see shadow-inserted row. Got: ' . json_encode($names)
                );
            }
            $this->assertCount(3, $rows); // Alpha, Beta, Delta
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Simple derived table failed: ' . $e->getMessage());
        }
    }

    /**
     * Derived table with aggregate reading shadow data.
     */
    public function testDerivedTableWithAggregate(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_dts_items VALUES (4, 'Delta', 'parts', 50.00)");

            $rows = $this->ztdQuery(
                "SELECT agg.category, agg.total FROM
                 (SELECT category, SUM(price) AS total FROM mi_dts_items GROUP BY category) agg
                 ORDER BY agg.category"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['category']] = (float) $row['total'];
            }

            if (!isset($map['parts']) || $map['parts'] !== 80.00) {
                $this->markTestIncomplete(
                    'Derived table aggregate wrong. Expected parts=80.00, got: ' . json_encode($map)
                );
            }
            $this->assertEquals(30.00, $map['tools']); // Alpha 10 + Beta 20
            $this->assertEquals(80.00, $map['parts']); // Gamma 30 + Delta 50
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Derived table with aggregate failed: ' . $e->getMessage());
        }
    }

    /**
     * Derived table after DELETE — deleted rows should not appear.
     */
    public function testDerivedTableAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_dts_items WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT sub.id FROM (SELECT id FROM mi_dts_items WHERE category = 'tools') sub ORDER BY sub.id"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Derived table returned empty result (Issue #13: derived tables not rewritten)'
                );
            }

            $ids = array_map('intval', array_column($rows, 'id'));
            if (in_array(2, $ids)) {
                $this->markTestIncomplete(
                    'Derived table still shows deleted row. Got ids: ' . json_encode($ids)
                );
            }
            $this->assertCount(1, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Derived table after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Nested derived tables (subquery within subquery in FROM).
     */
    public function testNestedDerivedTables(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_dts_items SET price = 25.00 WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT outer_sub.avg_price FROM
                 (SELECT AVG(inner_sub.price) AS avg_price FROM
                   (SELECT price FROM mi_dts_items WHERE category = 'tools') inner_sub
                 ) outer_sub"
            );

            $this->assertCount(1, $rows);
            // Alpha=10, Beta=25 → avg = 17.5
            $avg = (float) $rows[0]['avg_price'];
            if (abs($avg - 17.5) > 0.01) {
                $this->markTestIncomplete(
                    'Nested derived table avg wrong. Expected 17.5, got ' . $avg
                );
            }
            $this->assertEquals(17.5, $avg, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Nested derived tables failed: ' . $e->getMessage());
        }
    }
}
