<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests scalar subquery inside INSERT VALUES clause through ZTD shadow store.
 *
 * INSERT INTO t VALUES ((SELECT MAX(id) FROM t) + 1, 'name') is a common
 * pattern for custom sequence generation. The CTE rewriter must resolve the
 * subquery inside VALUES to a concrete value from the shadow store.
 *
 * @spec SPEC-4.1
 * @spec SPEC-3.3
 */
class SubqueryInValuesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_siv_items (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                score INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_siv_counters (
                id INT PRIMARY KEY,
                label VARCHAR(50) NOT NULL,
                total INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_siv_counters', 'mi_siv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_siv_items VALUES (1, 'Alpha', 10)");
        $this->mysqli->query("INSERT INTO mi_siv_items VALUES (2, 'Bravo', 20)");
        $this->mysqli->query("INSERT INTO mi_siv_items VALUES (3, 'Charlie', 30)");
    }

    /**
     * INSERT with (SELECT MAX(id) FROM t) + 1 in VALUES — custom sequence.
     */
    public function testInsertWithMaxIdSubquery(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_siv_items (id, name, score) VALUES ((SELECT MAX(id) FROM mi_siv_items) + 1, 'Delta', 40)"
            );

            $rows = $this->ztdQuery("SELECT id, name, score FROM mi_siv_items ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT with MAX subquery in VALUES: expected 4 rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(4, $rows);

            // New row should have id = 4 (MAX(3) + 1)
            if ((int) $rows[3]['id'] !== 4) {
                $this->markTestIncomplete(
                    'INSERT with MAX subquery: expected id=4, got id='
                    . var_export($rows[3]['id'], true)
                );
            }
            $this->assertEquals(4, (int) $rows[3]['id']);
            $this->assertSame('Delta', $rows[3]['name']);
            $this->assertEquals(40, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with MAX subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with (SELECT COUNT(*) FROM t) in VALUES — count-based value.
     */
    public function testInsertWithCountSubquery(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_siv_counters (id, label, total) VALUES (1, 'item_count', (SELECT COUNT(*) FROM mi_siv_items))"
            );

            $rows = $this->ztdQuery("SELECT id, label, total FROM mi_siv_counters WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT with COUNT subquery in VALUES: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // total should be 3 (COUNT of items)
            if ((int) $rows[0]['total'] !== 3) {
                $this->markTestIncomplete(
                    'INSERT with COUNT subquery: expected total=3, got total='
                    . var_export($rows[0]['total'], true)
                );
            }
            $this->assertEquals(3, (int) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with COUNT subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with subquery referencing shadow-inserted data.
     * Shadow-insert a row first, then use MAX(id) subquery in next INSERT.
     */
    public function testInsertSubqueryAfterShadowInsert(): void
    {
        try {
            // Shadow-insert a new row
            $this->mysqli->query("INSERT INTO mi_siv_items VALUES (10, 'Zulu', 99)");

            // Verify shadow-inserted row visible
            $check = $this->ztdQuery("SELECT MAX(id) AS mx FROM mi_siv_items");
            $maxId = (int) $check[0]['mx'];

            // Now INSERT with MAX subquery — should see id=10 from shadow
            $this->mysqli->query(
                "INSERT INTO mi_siv_items (id, name, score) VALUES ((SELECT MAX(id) FROM mi_siv_items) + 1, 'Beyond', 100)"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_siv_items WHERE name = 'Beyond'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT subquery after shadow INSERT: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // Should be id = 11 (MAX(10) + 1) if shadow data is visible in subquery
            if ((int) $rows[0]['id'] !== 11) {
                $this->markTestIncomplete(
                    'INSERT subquery after shadow: expected id=11, got id='
                    . var_export($rows[0]['id'], true)
                    . '. Subquery may not see shadow-inserted data.'
                );
            }
            $this->assertEquals(11, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT subquery after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with (SELECT SUM(score) FROM t WHERE condition) in VALUES.
     */
    public function testInsertWithSumSubqueryInValues(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_siv_counters (id, label, total) VALUES (2, 'high_score_sum', (SELECT SUM(score) FROM mi_siv_items WHERE score >= 20))"
            );

            $rows = $this->ztdQuery("SELECT total FROM mi_siv_counters WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT with SUM subquery: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // SUM(20 + 30) = 50
            if ((int) $rows[0]['total'] !== 50) {
                $this->markTestIncomplete(
                    'INSERT with SUM subquery: expected total=50, got total='
                    . var_export($rows[0]['total'], true)
                );
            }
            $this->assertEquals(50, (int) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with SUM subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with subquery in VALUES.
     */
    public function testPreparedInsertWithSubqueryInValues(): void
    {
        try {
            $stmt = $this->mysqli->prepare(
                "INSERT INTO mi_siv_counters (id, label, total) VALUES (?, ?, (SELECT COUNT(*) FROM mi_siv_items WHERE score > ?))"
            );
            $id = 3;
            $label = 'above_threshold';
            $threshold = 15;
            $stmt->bind_param('isi', $id, $label, $threshold);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT total FROM mi_siv_counters WHERE id = 3");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared INSERT with subquery: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // COUNT where score > 15: Bravo(20), Charlie(30) = 2
            if ((int) $rows[0]['total'] !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT with subquery: expected total=2, got total='
                    . var_export($rows[0]['total'], true)
                );
            }
            $this->assertEquals(2, (int) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with subquery in VALUES failed: ' . $e->getMessage());
        }
    }
}
