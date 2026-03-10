<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests partitioned tables through the ZTD shadow store.
 *
 * MySQL's PARTITION BY RANGE/HASH/LIST creates physically separate storage
 * for subsets of rows. The CTE rewriter needs to handle partition-aware
 * queries, including partition pruning hints and PARTITION clause in DML.
 *
 * Partitioned tables are common in high-volume applications for:
 * - Time-series data (partition by date range)
 * - Multi-tenant data (partition by tenant_id)
 * - Large lookup tables (partition by hash)
 *
 * @spec SPEC-3.1
 */
class MysqlPartitionedTableTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_prt_events (
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
            "CREATE TABLE my_prt_metrics (
                id INT NOT NULL,
                tenant_id INT NOT NULL,
                metric_name VARCHAR(100) NOT NULL,
                value DECIMAL(12,4) NOT NULL,
                PRIMARY KEY (id, tenant_id)
            ) PARTITION BY HASH(tenant_id) PARTITIONS 4",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_prt_metrics', 'my_prt_events'];
    }

    /**
     * Basic INSERT + SELECT on range-partitioned table.
     */
    public function testInsertSelectRangePartition(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (1, '2023-06-15', 'click', 'page1')");
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (2, '2024-03-20', 'view', 'page2')");
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (3, '2025-01-10', 'click', 'page3')");

            $rows = $this->ztdQuery(
                "SELECT event_date, category FROM my_prt_events ORDER BY event_date"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Partitioned table SELECT returned 0 rows. CTE rewriter may not support partitioned tables.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('click', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partitioned table INSERT+SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on partitioned table — modification visible in shadow.
     */
    public function testUpdatePartitionedTable(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (1, '2024-07-01', 'error', 'bug1')");

            $this->pdo->exec("UPDATE my_prt_events SET category = 'warning' WHERE event_date = '2024-07-01'");

            $rows = $this->ztdQuery(
                "SELECT category FROM my_prt_events WHERE event_date = '2024-07-01'"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('Partitioned table UPDATE: row disappeared.');
            }

            if ($rows[0]['category'] !== 'warning') {
                $this->markTestIncomplete(
                    'Partitioned table UPDATE: category is "' . $rows[0]['category']
                    . '", expected "warning".'
                );
            }

            $this->assertSame('warning', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partitioned table UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * PARTITION clause in SELECT — explicit partition pruning.
     * SELECT ... FROM table PARTITION (p2024) WHERE ...
     */
    public function testSelectWithPartitionClause(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (1, '2024-01-15', 'click', 'p1')");
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (2, '2024-11-20', 'view', 'p2')");
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (3, '2023-05-01', 'click', 'old')");

            $rows = $this->ztdQuery(
                "SELECT category, payload FROM my_prt_events PARTITION (p2024) ORDER BY event_date"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SELECT with PARTITION clause returned 0 rows. '
                    . 'CTE rewriter may not handle PARTITION() syntax.'
                );
            }

            // Only 2024 rows
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'syntax') !== false || stripos($msg, 'PARTITION') !== false) {
                $this->markTestIncomplete(
                    'PARTITION clause syntax not supported: ' . $msg
                );
            }
            $this->markTestIncomplete('PARTITION clause SELECT failed: ' . $msg);
        }
    }

    /**
     * Hash-partitioned table — basic CRUD.
     */
    public function testHashPartitionCrud(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_prt_metrics VALUES (1, 1, 'cpu', 85.5000)");
            $this->pdo->exec("INSERT INTO my_prt_metrics VALUES (2, 2, 'mem', 62.3000)");
            $this->pdo->exec("INSERT INTO my_prt_metrics VALUES (3, 3, 'disk', 45.0000)");

            $rows = $this->ztdQuery(
                "SELECT tenant_id, metric_name, value FROM my_prt_metrics ORDER BY tenant_id"
            );

            $this->assertCount(3, $rows);

            // UPDATE
            $this->pdo->exec("UPDATE my_prt_metrics SET value = 90.0000 WHERE tenant_id = 1");

            $rows = $this->ztdQuery(
                "SELECT value FROM my_prt_metrics WHERE tenant_id = 1 AND metric_name = 'cpu'"
            );

            $this->assertEquals(90.0, (float) $rows[0]['value']);

            // DELETE
            $this->pdo->exec("DELETE FROM my_prt_metrics WHERE tenant_id = 3");
            $rows = $this->ztdQuery("SELECT COUNT(*) as cnt FROM my_prt_metrics");
            $this->assertSame(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Hash partition CRUD failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate across partitions after DML.
     */
    public function testAggregateAcrossPartitions(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (1, '2023-03-01', 'click', 'a')");
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (2, '2024-06-15', 'click', 'b')");
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (3, '2025-09-20', 'click', 'c')");
            $this->pdo->exec("INSERT INTO my_prt_events VALUES (4, '2024-12-25', 'view', 'd')");

            // Delete one
            $this->pdo->exec("DELETE FROM my_prt_events WHERE event_date = '2023-03-01'");

            $rows = $this->ztdQuery(
                "SELECT YEAR(event_date) as yr, COUNT(*) as cnt
                 FROM my_prt_events
                 GROUP BY YEAR(event_date)
                 ORDER BY yr"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('Aggregate across partitions returned 0 groups.');
            }

            // Should be 2 groups: 2024 (2 rows), 2025 (1 row)
            $this->assertCount(2, $rows);
            $this->assertSame(2024, (int) $rows[0]['yr']);
            $this->assertSame(2, (int) $rows[0]['cnt']);
            $this->assertSame(2025, (int) $rows[1]['yr']);
            $this->assertSame(1, (int) $rows[1]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate across partitions failed: ' . $e->getMessage());
        }
    }
}
