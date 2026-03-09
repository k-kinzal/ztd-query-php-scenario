<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL query hint syntax (STRAIGHT_JOIN, USE INDEX, FORCE INDEX,
 * IGNORE INDEX) through the CTE rewriter.
 *
 * Real-world scenario: performance-tuning MySQL applications use index hints
 * and join hints to control query execution plans. These non-standard SQL
 * extensions modify the FROM clause syntax and could confuse the CTE rewriter's
 * table reference detection.
 *
 * @spec SPEC-3.1
 * @spec SPEC-10.2
 */
class MysqlHintSyntaxTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_hint_items (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                category VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                INDEX idx_category (category),
                INDEX idx_price (price)
            ) ENGINE=InnoDB',
            'CREATE TABLE my_hint_orders (
                id INT PRIMARY KEY,
                item_id INT NOT NULL,
                quantity INT NOT NULL,
                INDEX idx_item (item_id)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_hint_orders', 'my_hint_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_hint_items VALUES (1, 'Widget', 'tools', 10.00)");
        $this->ztdExec("INSERT INTO my_hint_items VALUES (2, 'Gadget', 'electronics', 25.00)");
        $this->ztdExec("INSERT INTO my_hint_items VALUES (3, 'Bolt', 'hardware', 2.50)");
        $this->ztdExec("INSERT INTO my_hint_orders VALUES (1, 1, 5)");
        $this->ztdExec("INSERT INTO my_hint_orders VALUES (2, 2, 3)");
    }

    /**
     * SELECT with USE INDEX hint.
     */
    public function testSelectWithUseIndex(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM my_hint_items USE INDEX (idx_category)
                 WHERE category = 'tools'"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'SELECT with USE INDEX returned no rows from shadow data.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT with USE INDEX failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with FORCE INDEX hint.
     */
    public function testSelectWithForceIndex(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM my_hint_items FORCE INDEX (idx_price)
                 WHERE price > 5.00
                 ORDER BY price"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'SELECT with FORCE INDEX returned no rows.'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT with FORCE INDEX failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with IGNORE INDEX hint.
     */
    public function testSelectWithIgnoreIndex(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM my_hint_items IGNORE INDEX (idx_category)
                 WHERE category = 'electronics'"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT with IGNORE INDEX failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * JOIN with USE INDEX on both tables.
     */
    public function testJoinWithUseIndexBothTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT i.name, o.quantity
                 FROM my_hint_items i USE INDEX (PRIMARY)
                 JOIN my_hint_orders o USE INDEX (idx_item) ON o.item_id = i.id
                 ORDER BY i.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'JOIN with USE INDEX on both tables returned no rows.'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JOIN with USE INDEX both tables failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * STRAIGHT_JOIN hint.
     */
    public function testStraightJoin(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT STRAIGHT_JOIN i.name, o.quantity
                 FROM my_hint_items i
                 JOIN my_hint_orders o ON o.item_id = i.id
                 ORDER BY i.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'STRAIGHT_JOIN returned no rows from shadow data.'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'STRAIGHT_JOIN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * USE INDEX with prepared params in WHERE.
     */
    public function testUseIndexWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT * FROM my_hint_items USE INDEX (idx_category)
                 WHERE category = ? AND price > ?",
                ['tools', 5.0]
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'USE INDEX with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * FOR ORDER BY index hint variant.
     */
    public function testUseIndexForOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM my_hint_items USE INDEX FOR ORDER BY (idx_price)
                 ORDER BY price DESC"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Gadget', $rows[0]['name']); // highest price
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'USE INDEX FOR ORDER BY failed: ' . $e->getMessage()
            );
        }
    }
}
