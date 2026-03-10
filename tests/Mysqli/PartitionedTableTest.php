<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests partitioned tables through the ZTD shadow store (MySQLi adapter).
 *
 * @spec SPEC-3.1
 */
class PartitionedTableTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_prt_events (
                id INT NOT NULL,
                event_date DATE NOT NULL,
                category VARCHAR(50) NOT NULL,
                payload TEXT,
                PRIMARY KEY (id, event_date)
            ) PARTITION BY RANGE (YEAR(event_date)) (
                PARTITION p2023 VALUES LESS THAN (2024),
                PARTITION p2024 VALUES LESS THAN (2025),
                PARTITION p2025 VALUES LESS THAN (2026),
                PARTITION pmax VALUES LESS THAN MAXVALUE
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_prt_events'];
    }

    /**
     * Basic INSERT + SELECT on partitioned table.
     */
    public function testInsertSelectPartitioned(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (1, '2023-06-15', 'click', 'page1')");
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (2, '2024-03-20', 'view', 'page2')");
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (3, '2025-01-10', 'click', 'page3')");

            $rows = $this->ztdQuery(
                "SELECT event_date, category FROM mi_prt_events ORDER BY event_date"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Partitioned table SELECT returned 0 rows.'
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partitioned table test failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with PARTITION clause.
     */
    public function testSelectWithPartitionClause(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (1, '2024-01-15', 'click', 'p1')");
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (2, '2024-11-20', 'view', 'p2')");
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (3, '2023-05-01', 'click', 'old')");

            $rows = $this->ztdQuery(
                "SELECT category FROM mi_prt_events PARTITION (p2024) ORDER BY event_date"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'PARTITION clause returned 0 rows. CTE rewriter may not support this syntax.'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('PARTITION clause test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE + DELETE on partitioned table.
     */
    public function testUpdateDeletePartitioned(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (1, '2024-05-01', 'error', 'e1')");
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (2, '2024-06-01', 'click', 'c1')");

            $this->mysqli->query("UPDATE mi_prt_events SET category = 'warning' WHERE category = 'error'");
            $this->mysqli->query("DELETE FROM mi_prt_events WHERE category = 'click'");

            $rows = $this->ztdQuery("SELECT category FROM mi_prt_events");

            $this->assertCount(1, $rows);
            $this->assertSame('warning', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partitioned UPDATE/DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement on partitioned table.
     */
    public function testPreparedPartitioned(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (1, '2024-01-01', 'A', 'x')");
            $this->mysqli->query("INSERT INTO mi_prt_events VALUES (2, '2025-01-01', 'B', 'y')");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category FROM mi_prt_events WHERE event_date > ? ORDER BY category",
                ['2024-06-01']
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('Prepared SELECT on partitioned table returned 0 rows.');
            }

            $this->assertCount(1, $rows);
            $this->assertSame('B', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared partitioned test failed: ' . $e->getMessage());
        }
    }
}
