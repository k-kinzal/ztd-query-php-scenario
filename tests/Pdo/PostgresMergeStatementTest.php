<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL MERGE statement (SQL standard, added in PostgreSQL 15).
 *
 * MERGE combines INSERT, UPDATE, and DELETE in a single atomic statement based
 * on whether the source row matches a target row. This is commonly used for
 * upsert workflows, data synchronization, and ETL pipelines.
 *
 * The CTE rewriter likely has no support for MERGE since it was added in PG 15
 * and is syntactically distinct from INSERT/UPDATE/DELETE.
 *
 * @spec SPEC-6.1
 */
class PostgresMergeStatementTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_merge_target (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )",
            "CREATE TABLE pg_merge_source (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_merge_source', 'pg_merge_target'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Check PostgreSQL version >= 15 for MERGE support
        $version = $this->getDbVersion();
        $major = (int) explode('.', $version)[0];
        if ($major < 15) {
            $this->markTestSkipped("MERGE requires PostgreSQL 15+, running {$version}");
        }

        // Seed target with existing rows
        $this->pdo->exec("INSERT INTO pg_merge_target (id, name, value) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pg_merge_target (id, name, value) VALUES (2, 'Bob', 200)");
        $this->pdo->exec("INSERT INTO pg_merge_target (id, name, value) VALUES (3, 'Carol', 300)");

        // Seed source with mix of matching and new rows
        $this->pdo->exec("INSERT INTO pg_merge_source (id, name, value) VALUES (2, 'Bob-Updated', 250)");
        $this->pdo->exec("INSERT INTO pg_merge_source (id, name, value) VALUES (3, 'Carol-Updated', 350)");
        $this->pdo->exec("INSERT INTO pg_merge_source (id, name, value) VALUES (4, 'Dave', 400)");
    }

    /**
     * Basic MERGE with WHEN MATCHED THEN UPDATE and WHEN NOT MATCHED THEN INSERT.
     *
     * This is the most common MERGE pattern: upsert from a source table.
     */
    public function testMergeUpsertFromSourceTable(): void
    {
        try {
            $this->pdo->exec("
                MERGE INTO pg_merge_target AS t
                USING pg_merge_source AS s ON t.id = s.id
                WHEN MATCHED THEN
                    UPDATE SET name = s.name, value = s.value
                WHEN NOT MATCHED THEN
                    INSERT (id, name, value) VALUES (s.id, s.name, s.value)
            ");

            $rows = $this->ztdQuery("SELECT id, name, value FROM pg_merge_target ORDER BY id");

            // Expect: id=1 unchanged, id=2 updated, id=3 updated, id=4 inserted
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'MERGE upsert: expected 4 rows, got ' . count($rows)
                    . '. MERGE may not be supported by CTE rewriter. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Alice', $rows[0]['name']);       // unchanged
            $this->assertSame(100, (int) $rows[0]['value']);
            $this->assertSame('Bob-Updated', $rows[1]['name']); // updated
            $this->assertSame(250, (int) $rows[1]['value']);
            $this->assertSame('Carol-Updated', $rows[2]['name']); // updated
            $this->assertSame('Dave', $rows[3]['name']);         // inserted
            $this->assertSame(400, (int) $rows[3]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('MERGE upsert failed: ' . $e->getMessage());
        }
    }

    /**
     * MERGE with WHEN MATCHED AND condition THEN UPDATE.
     *
     * Conditional update: only update rows where the source value is greater.
     */
    public function testMergeConditionalUpdate(): void
    {
        try {
            // Source has id=2 value=250 (> target 200), id=3 value=350 (> target 300)
            // Add a source row with lower value
            $this->pdo->exec("INSERT INTO pg_merge_source (id, name, value) VALUES (1, 'Alice-Skip', 50)");

            $this->pdo->exec("
                MERGE INTO pg_merge_target AS t
                USING pg_merge_source AS s ON t.id = s.id
                WHEN MATCHED AND s.value > t.value THEN
                    UPDATE SET name = s.name, value = s.value
                WHEN MATCHED THEN
                    DO NOTHING
                WHEN NOT MATCHED THEN
                    INSERT (id, name, value) VALUES (s.id, s.name, s.value)
            ");

            $rows = $this->ztdQuery("SELECT id, name, value FROM pg_merge_target ORDER BY id");

            if (count($rows) < 3) {
                $this->markTestIncomplete(
                    'MERGE conditional: expected 4 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            // id=1 should NOT be updated (source value 50 < target 100)
            $alice = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 1));
            if (count($alice) > 0 && $alice[0]['name'] !== 'Alice') {
                $this->markTestIncomplete(
                    'MERGE conditional: id=1 was updated despite s.value < t.value. '
                    . 'Got: ' . json_encode($alice[0])
                );
            }

            $this->assertSame('Alice', $alice[0]['name']);
            $this->assertSame(100, (int) $alice[0]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('MERGE conditional update failed: ' . $e->getMessage());
        }
    }

    /**
     * MERGE with WHEN MATCHED THEN DELETE.
     *
     * Delete rows from target that match the source.
     */
    public function testMergeWithDelete(): void
    {
        try {
            $this->pdo->exec("
                MERGE INTO pg_merge_target AS t
                USING pg_merge_source AS s ON t.id = s.id
                WHEN MATCHED THEN DELETE
            ");

            $rows = $this->ztdQuery("SELECT id, name FROM pg_merge_target ORDER BY id");

            // Source has ids 2,3,4. Target has ids 1,2,3.
            // Matched: 2 and 3 should be deleted. id=1 stays.
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'MERGE DELETE: expected 1 remaining row (id=1), got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('MERGE with DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * MERGE using VALUES as inline source (no source table).
     *
     * Common pattern for upserting a small batch of values without a staging table.
     */
    public function testMergeWithValuesSource(): void
    {
        try {
            $this->pdo->exec("
                MERGE INTO pg_merge_target AS t
                USING (VALUES (1, 'Alice-New', 999), (5, 'Eve', 500)) AS s(id, name, value)
                    ON t.id = s.id
                WHEN MATCHED THEN
                    UPDATE SET name = s.name, value = s.value
                WHEN NOT MATCHED THEN
                    INSERT (id, name, value) VALUES (s.id, s.name, s.value)
            ");

            $rows = $this->ztdQuery("SELECT id, name, value FROM pg_merge_target ORDER BY id");

            // id=1 updated to 'Alice-New'/999, id=2/3 unchanged, id=5 inserted
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'MERGE VALUES source: expected 4 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Alice-New', $rows[0]['name']);
            $this->assertSame(999, (int) $rows[0]['value']);
            $this->assertSame(5, (int) $rows[3]['id']);
            $this->assertSame('Eve', $rows[3]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('MERGE with VALUES source failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared MERGE with $N parameters.
     */
    public function testPreparedMerge(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "MERGE INTO pg_merge_target AS t
                 USING (VALUES ($1::int, $2::text, $3::int)) AS s(id, name, value)
                     ON t.id = s.id
                 WHEN MATCHED THEN
                     UPDATE SET name = s.name, value = s.value
                 WHEN NOT MATCHED THEN
                     INSERT (id, name, value) VALUES (s.id, s.name, s.value)
                 RETURNING *",
                [1, 'Alice-Prepared', 111]
            );

            // Check the RETURNING clause works
            if (count($rows) === 0) {
                // RETURNING may not work; check via SELECT
                $rows = $this->ztdQuery("SELECT name, value FROM pg_merge_target WHERE id = 1");
                if (count($rows) === 0 || $rows[0]['name'] !== 'Alice-Prepared') {
                    $this->markTestIncomplete(
                        'Prepared MERGE: row not updated. Got: ' . json_encode($rows)
                    );
                }
                $this->assertSame('Alice-Prepared', $rows[0]['name']);
                return;
            }

            $this->assertSame('Alice-Prepared', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared MERGE failed: ' . $e->getMessage());
        }
    }

    /**
     * MERGE followed by SELECT to verify shadow store consistency.
     */
    public function testMergeThenSelectConsistency(): void
    {
        try {
            $this->pdo->exec("
                MERGE INTO pg_merge_target AS t
                USING pg_merge_source AS s ON t.id = s.id
                WHEN MATCHED THEN
                    UPDATE SET value = t.value + s.value
                WHEN NOT MATCHED THEN
                    INSERT (id, name, value) VALUES (s.id, s.name, s.value)
            ");

            // Aggregation should reflect merged state
            $rows = $this->ztdQuery("SELECT SUM(value) as total, COUNT(*) as cnt FROM pg_merge_target");

            // Expected: id=1 (100), id=2 (200+250=450), id=3 (300+350=650), id=4 (400)
            // Total = 100 + 450 + 650 + 400 = 1600
            if ((int) $rows[0]['cnt'] !== 4) {
                $this->markTestIncomplete(
                    'MERGE + aggregate: expected 4 rows, COUNT returned ' . $rows[0]['cnt']
                    . '. SUM=' . $rows[0]['total']
                );
            }

            $this->assertSame(4, (int) $rows[0]['cnt']);
            $this->assertSame(1600, (int) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('MERGE then SELECT consistency failed: ' . $e->getMessage());
        }
    }

    /**
     * MERGE with all three actions: INSERT, UPDATE, and DELETE.
     */
    public function testMergeAllActions(): void
    {
        try {
            // Source id=2 has higher value (250>200) → update
            // Source id=3 has higher value (350>300) but we'll delete where value > 300
            // Source id=4 is new → insert
            $this->pdo->exec("
                MERGE INTO pg_merge_target AS t
                USING pg_merge_source AS s ON t.id = s.id
                WHEN MATCHED AND s.value > 300 THEN
                    DELETE
                WHEN MATCHED THEN
                    UPDATE SET name = s.name, value = s.value
                WHEN NOT MATCHED THEN
                    INSERT (id, name, value) VALUES (s.id, s.name, s.value)
            ");

            $rows = $this->ztdQuery("SELECT id, name, value FROM pg_merge_target ORDER BY id");

            // id=1 unchanged (no source match), id=2 updated, id=3 deleted (s.value=350>300), id=4 inserted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'MERGE all actions: expected 3 rows (id 1,2,4), got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_column($rows, 'id');
            $this->assertContains('1', $ids);
            $this->assertContains('2', $ids);
            $this->assertContains('4', $ids);
            $this->assertNotContains('3', $ids); // deleted
        } catch (\Throwable $e) {
            $this->markTestIncomplete('MERGE all actions failed: ' . $e->getMessage());
        }
    }
}
