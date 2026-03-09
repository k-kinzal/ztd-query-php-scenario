<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests rapid sequential exec() calls and their cumulative effect on SQLite.
 *
 * Real-world scenario: batch processing scripts execute many DML statements
 * in sequence. The shadow store must accumulate changes correctly and
 * maintain consistency after 10+ sequential mutations.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteMultipleExecSequenceTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mes_counters (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['mes_counters'];
    }

    /**
     * 20 sequential INSERTs then aggregate query.
     */
    public function testTwentySequentialInserts(): void
    {
        for ($i = 1; $i <= 20; $i++) {
            $this->ztdExec("INSERT INTO mes_counters VALUES ($i, 'Item$i', $i)");
        }

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt, SUM(count) AS total FROM mes_counters");
        $this->assertSame(20, (int) $rows[0]['cnt']);
        // SUM(1..20) = 210
        $this->assertSame(210, (int) $rows[0]['total']);
    }

    /**
     * INSERT all, then UPDATE all, then verify.
     */
    public function testInsertAllThenUpdateAll(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("INSERT INTO mes_counters VALUES ($i, 'C$i', 0)");
        }

        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("UPDATE mes_counters SET count = $i * 10 WHERE id = $i");
        }

        $rows = $this->ztdQuery("SELECT SUM(count) AS total FROM mes_counters");
        // SUM(10+20+30+...+100) = 550
        $this->assertSame(550, (int) $rows[0]['total']);

        // Verify individual row
        $rows = $this->ztdQuery("SELECT count FROM mes_counters WHERE id = 5");
        $this->assertSame(50, (int) $rows[0]['count']);
    }

    /**
     * INSERT, UPDATE same row 10 times, verify final value.
     */
    public function testRepeatedUpdatesSameRow(): void
    {
        $this->ztdExec("INSERT INTO mes_counters VALUES (1, 'Counter', 0)");

        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("UPDATE mes_counters SET count = $i WHERE id = 1");
        }

        $rows = $this->ztdQuery("SELECT count FROM mes_counters WHERE id = 1");
        $this->assertSame(10, (int) $rows[0]['count']);
    }

    /**
     * INSERT 10, DELETE odd IDs, verify even remain.
     */
    public function testInsertThenDeletePattern(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("INSERT INTO mes_counters VALUES ($i, 'C$i', $i)");
        }

        for ($i = 1; $i <= 10; $i += 2) {
            $this->ztdExec("DELETE FROM mes_counters WHERE id = $i");
        }

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mes_counters");
        $this->assertSame(5, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT id FROM mes_counters ORDER BY id");
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([2, 4, 6, 8, 10], $ids);
    }

    /**
     * INSERT, DELETE, re-INSERT same PK, repeated pattern.
     */
    public function testDeleteReinsertCycle(): void
    {
        for ($cycle = 1; $cycle <= 5; $cycle++) {
            $this->ztdExec("INSERT INTO mes_counters VALUES (1, 'Cycle$cycle', $cycle)");
            if ($cycle < 5) {
                $this->ztdExec("DELETE FROM mes_counters WHERE id = 1");
            }
        }

        $rows = $this->ztdQuery("SELECT name, count FROM mes_counters WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Cycle5', $rows[0]['name']);
        $this->assertSame(5, (int) $rows[0]['count']);
    }

    /**
     * Interleaved INSERT and SELECT — each SELECT sees cumulative inserts.
     */
    public function testInterleavedInsertSelect(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->ztdExec("INSERT INTO mes_counters VALUES ($i, 'C$i', $i)");
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mes_counters");
            $this->assertSame($i, (int) $rows[0]['cnt'], "After INSERT #$i, expected $i rows");
        }
    }
}
