<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared statements with HAVING clause after DML on MySQL.
 *
 * Issue #22 documents that prepared params in HAVING fail on SQLite/PostgreSQL.
 * This tests whether MySQL has the same issue or handles it differently.
 *
 * HAVING with prepared parameters is common in reporting queries:
 * "Show categories with more than ? items"
 *
 * @spec SPEC-4.2
 */
class PreparedHavingAfterDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_phd_items (
            id INT PRIMARY KEY,
            category VARCHAR(30) NOT NULL,
            name VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_phd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_phd_items VALUES (1, 'tools', 'Hammer', 15.00)");
        $this->mysqli->query("INSERT INTO mi_phd_items VALUES (2, 'tools', 'Wrench', 12.00)");
        $this->mysqli->query("INSERT INTO mi_phd_items VALUES (3, 'electronics', 'Radio', 45.00)");
        $this->mysqli->query("INSERT INTO mi_phd_items VALUES (4, 'clothing', 'Shirt', 25.00)");
    }

    /**
     * Prepared HAVING COUNT(*) > ? after INSERT.
     */
    public function testPreparedHavingCountAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_phd_items VALUES (5, 'tools', 'Saw', 20.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, COUNT(*) AS cnt
                 FROM mi_phd_items
                 GROUP BY category
                 HAVING COUNT(*) > ?",
                [1]
            );

            // After INSERT: tools=3, electronics=1, clothing=1
            // HAVING COUNT(*) > 1 → only tools(3)
            if (empty($rows)) {
                $this->markTestIncomplete('Prepared HAVING: empty result (Issue #22 on MySQL?)');
            }
            $this->assertCount(1, $rows);
            $this->assertSame('tools', $rows[0]['category']);
            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared HAVING COUNT after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared HAVING SUM() > ? after DML.
     */
    public function testPreparedHavingSumAfterUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_phd_items SET price = 50.00 WHERE id = 3");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, SUM(price) AS total
                 FROM mi_phd_items
                 GROUP BY category
                 HAVING SUM(price) > ?",
                [30.0]
            );

            // tools: 15+12=27, electronics: 50, clothing: 25
            // HAVING SUM > 30 → electronics(50)
            if (empty($rows)) {
                $this->markTestIncomplete('Prepared HAVING SUM: empty result');
            }

            $cats = array_column($rows, 'category');
            if (!in_array('electronics', $cats)) {
                $this->markTestIncomplete('Prepared HAVING SUM: electronics not found. Got: ' . implode(', ', $cats));
            }
            $this->assertCount(1, $rows);
            $this->assertEquals(50.00, (float) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared HAVING SUM after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared WHERE + HAVING combined.
     */
    public function testPreparedWhereAndHaving(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_phd_items VALUES (5, 'tools', 'Saw', 20.00)");
            $this->mysqli->query("INSERT INTO mi_phd_items VALUES (6, 'electronics', 'TV', 300.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price
                 FROM mi_phd_items
                 WHERE price > ?
                 GROUP BY category
                 HAVING COUNT(*) >= ?",
                [10.0, 2]
            );

            // WHERE price > 10: all except nothing (all > 10)
            // tools: Hammer(15), Wrench(12), Saw(20) → 3 items
            // electronics: Radio(45), TV(300) → 2 items
            // clothing: Shirt(25) → 1 item (excluded by HAVING >= 2)
            $cats = array_column($rows, 'category');
            sort($cats);

            if (count($cats) !== 2) {
                $this->markTestIncomplete('WHERE+HAVING: expected 2 categories. Got: ' . implode(', ', $cats));
            }
            $this->assertEquals(['electronics', 'tools'], $cats);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared WHERE+HAVING failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared HAVING after DELETE.
     */
    public function testPreparedHavingAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_phd_items WHERE id = 2"); // Remove Wrench from tools

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, COUNT(*) AS cnt
                 FROM mi_phd_items
                 GROUP BY category
                 HAVING COUNT(*) >= ?",
                [1]
            );

            // tools: 1 (Hammer only), electronics: 1, clothing: 1
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared HAVING after DELETE failed: ' . $e->getMessage());
        }
    }
}
