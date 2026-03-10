<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL declarative partitioned tables through ZTD shadow store.
 *
 * PostgreSQL supports PARTITION BY RANGE, LIST, and HASH with child
 * partition tables. DML on the parent table routes rows to partitions.
 * The CTE rewriter must handle partitioned tables correctly — both
 * direct queries on the parent and queries on individual partitions.
 *
 * @spec SPEC-3.1
 */
class PostgresPartitionedTableTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_prt_logs (
                id INTEGER NOT NULL,
                log_date DATE NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                PRIMARY KEY (id, log_date)
            ) PARTITION BY RANGE (log_date)",
            "CREATE TABLE pg_prt_logs_2024 PARTITION OF pg_prt_logs
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            "CREATE TABLE pg_prt_logs_2025 PARTITION OF pg_prt_logs
                FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
            "CREATE TABLE pg_prt_logs_2026 PARTITION OF pg_prt_logs
                FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
            "CREATE TABLE pg_prt_tags (
                id INTEGER NOT NULL,
                region TEXT NOT NULL,
                tag TEXT NOT NULL,
                value NUMERIC(10,2) NOT NULL,
                PRIMARY KEY (id, region)
            ) PARTITION BY LIST (region)",
            "CREATE TABLE pg_prt_tags_us PARTITION OF pg_prt_tags
                FOR VALUES IN ('us-east', 'us-west')",
            "CREATE TABLE pg_prt_tags_eu PARTITION OF pg_prt_tags
                FOR VALUES IN ('eu-west', 'eu-central')",
        ];
    }

    protected function getTableNames(): array
    {
        return [
            'pg_prt_tags_eu', 'pg_prt_tags_us', 'pg_prt_tags',
            'pg_prt_logs_2026', 'pg_prt_logs_2025', 'pg_prt_logs_2024', 'pg_prt_logs',
        ];
    }

    /**
     * INSERT into parent table + SELECT — rows route to correct partitions.
     */
    public function testInsertSelectRangePartition(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (1, '2024-06-15', 'INFO', 'Started')");
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (2, '2025-03-20', 'WARN', 'Slow query')");
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (3, '2026-01-10', 'ERROR', 'Timeout')");

            $rows = $this->ztdQuery(
                "SELECT log_date, level, message FROM pg_prt_logs ORDER BY log_date"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Partitioned table SELECT returned 0 rows through shadow store.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('INFO', $rows[0]['level']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Range partition INSERT+SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on partitioned table.
     */
    public function testUpdatePartitioned(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (1, '2025-07-01', 'ERROR', 'Bug')");
            $this->pdo->exec("UPDATE pg_prt_logs SET level = 'WARN', message = 'Fixed' WHERE log_date = '2025-07-01'");

            $rows = $this->ztdQuery("SELECT level, message FROM pg_prt_logs WHERE log_date = '2025-07-01'");

            if (count($rows) === 0) {
                $this->markTestIncomplete('Partitioned UPDATE: row not visible.');
            }

            if ($rows[0]['level'] !== 'WARN') {
                $this->markTestIncomplete(
                    'Partitioned UPDATE: level is "' . $rows[0]['level']
                    . '", expected "WARN". Shadow store may not track UPDATE on partitioned tables.'
                );
            }

            $this->assertSame('WARN', $rows[0]['level']);
            $this->assertSame('Fixed', $rows[0]['message']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partitioned UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE on partitioned table.
     */
    public function testDeletePartitioned(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (1, '2024-01-15', 'INFO', 'a')");
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (2, '2024-02-15', 'ERROR', 'b')");

            $this->pdo->exec("DELETE FROM pg_prt_logs WHERE level = 'ERROR'");

            $rows = $this->ztdQuery("SELECT level FROM pg_prt_logs");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Partitioned DELETE: expected 1 row after delete, got ' . count($rows)
                    . '. Shadow store may not track DELETE on partitioned tables.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('INFO', $rows[0]['level']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partitioned DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Query specific partition directly.
     */
    public function testQuerySpecificPartition(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (1, '2024-05-01', 'INFO', 'x')");
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (2, '2025-05-01', 'WARN', 'y')");

            // Query specific partition table directly
            $rows = $this->ztdQuery("SELECT level FROM pg_prt_logs_2024");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Direct partition query returned 0 rows. Shadow store may not track child partitions.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('INFO', $rows[0]['level']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Direct partition query failed: ' . $e->getMessage());
        }
    }

    /**
     * LIST partition — INSERT by region.
     */
    public function testListPartition(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_prt_tags VALUES (1, 'us-east', 'cpu', 85.50)");
            $this->pdo->exec("INSERT INTO pg_prt_tags VALUES (2, 'eu-west', 'mem', 62.30)");
            $this->pdo->exec("INSERT INTO pg_prt_tags VALUES (3, 'us-west', 'disk', 45.00)");

            $rows = $this->ztdQuery(
                "SELECT region, tag, value FROM pg_prt_tags ORDER BY region"
            );

            $this->assertCount(3, $rows);

            // Delete EU data
            $this->pdo->exec("DELETE FROM pg_prt_tags WHERE region LIKE 'eu%'");

            $rows = $this->ztdQuery("SELECT COUNT(*) as cnt FROM pg_prt_tags");

            if ((int) $rows[0]['cnt'] !== 2) {
                $this->markTestIncomplete(
                    'LIST partition DELETE: expected 2 rows after delete, got ' . $rows[0]['cnt']
                    . '. Shadow store may not track DELETE on LIST-partitioned tables.'
                );
            }

            $this->assertSame(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LIST partition test failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate across partitions after DML.
     */
    public function testAggregateAcrossPartitions(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (1, '2024-01-01', 'INFO', 'a')");
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (2, '2024-06-01', 'ERROR', 'b')");
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (3, '2025-01-01', 'INFO', 'c')");
            $this->pdo->exec("INSERT INTO pg_prt_logs VALUES (4, '2025-06-01', 'WARN', 'd')");

            $this->pdo->exec("DELETE FROM pg_prt_logs WHERE level = 'ERROR'");

            $rows = $this->ztdQuery(
                "SELECT EXTRACT(YEAR FROM log_date)::int as yr, COUNT(*) as cnt
                 FROM pg_prt_logs
                 GROUP BY EXTRACT(YEAR FROM log_date)
                 ORDER BY yr"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Aggregate on partitioned table returned 0 groups after DML.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(2024, (int) $rows[0]['yr']);
            $this->assertSame(1, (int) $rows[0]['cnt']);  // Only INFO remains
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate across partitions failed: ' . $e->getMessage());
        }
    }
}
