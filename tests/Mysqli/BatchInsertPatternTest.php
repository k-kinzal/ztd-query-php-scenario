<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multiple sequential INSERTs, batch insert visibility, and large dataset
 * insertion then aggregation through the ZTD shadow store (MySQLi).
 * Empty tables at start - tests INSERT-then-read pattern from scratch.
 * @spec SPEC-10.2.94
 */
class BatchInsertPatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_bi_log_entries (
            id INT PRIMARY KEY,
            level VARCHAR(20),
            message VARCHAR(500),
            source VARCHAR(100),
            created_at DATETIME
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_bi_log_entries'];
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testSequentialInserts(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $level = match ($i % 3) {
                0 => 'ERROR',
                1 => 'INFO',
                2 => 'DEBUG',
            };
            $this->mysqli->query(
                "INSERT INTO mi_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:0$i')"
            );
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_bi_log_entries');
        $this->assertSame(10, (int) $rows[0]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testInsertThenFilter(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $level = match ($i % 3) {
                0 => 'ERROR',
                1 => 'INFO',
                2 => 'DEBUG',
            };
            $this->mysqli->query(
                "INSERT INTO mi_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        // ERROR: ids 3, 6, 9 = 3 rows
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_bi_log_entries WHERE level = 'ERROR'");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        // INFO: ids 1, 4, 7, 10 = 4 rows
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_bi_log_entries WHERE level = 'INFO'");
        $this->assertSame(4, (int) $rows[0]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testInsertAndAggregate(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $level = match ($i % 3) {
                0 => 'ERROR',
                1 => 'INFO',
                2 => 'DEBUG',
            };
            $this->mysqli->query(
                "INSERT INTO mi_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        $rows = $this->ztdQuery(
            "SELECT level, COUNT(*) AS cnt FROM mi_bi_log_entries GROUP BY level ORDER BY level"
        );

        $this->assertCount(3, $rows);
        // DEBUG: ids 2, 5, 8 = 3
        $this->assertSame('DEBUG', $rows[0]['level']);
        $this->assertSame(3, (int) $rows[0]['cnt']);
        // ERROR: ids 3, 6, 9 = 3
        $this->assertSame('ERROR', $rows[1]['level']);
        $this->assertSame(3, (int) $rows[1]['cnt']);
        // INFO: ids 1, 4, 7, 10 = 4
        $this->assertSame('INFO', $rows[2]['level']);
        $this->assertSame(4, (int) $rows[2]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testInsertUpdateRead(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->mysqli->query(
                "INSERT INTO mi_bi_log_entries VALUES ($i, 'INFO', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        // Update some to ERROR
        $this->mysqli->query("UPDATE mi_bi_log_entries SET level = 'ERROR' WHERE id <= 2");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_bi_log_entries WHERE level = 'ERROR'");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_bi_log_entries WHERE level = 'INFO'");
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testInsertDeleteRead(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $level = match ($i % 3) {
                0 => 'ERROR',
                1 => 'INFO',
                2 => 'DEBUG',
            };
            $this->mysqli->query(
                "INSERT INTO mi_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        // Delete all DEBUG entries (ids 2, 5, 8 = 3 rows)
        $this->mysqli->query("DELETE FROM mi_bi_log_entries WHERE level = 'DEBUG'");
        $this->assertSame(3, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_bi_log_entries');
        $this->assertSame(7, (int) $rows[0]['cnt']);

        // Verify no DEBUG remaining
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_bi_log_entries WHERE level = 'DEBUG'");
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testMixedDmlVisibility(): void
    {
        // INSERT 5
        for ($i = 1; $i <= 5; $i++) {
            $this->mysqli->query(
                "INSERT INTO mi_bi_log_entries VALUES ($i, 'INFO', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        // UPDATE 2 to ERROR
        $this->mysqli->query("UPDATE mi_bi_log_entries SET level = 'ERROR' WHERE id IN (1, 2)");

        // DELETE 1
        $this->mysqli->query("DELETE FROM mi_bi_log_entries WHERE id = 3");

        // INSERT 3 more
        $this->mysqli->query("INSERT INTO mi_bi_log_entries VALUES (6, 'WARN', 'Message 6', 'api', '2026-03-09 11:00:00')");
        $this->mysqli->query("INSERT INTO mi_bi_log_entries VALUES (7, 'DEBUG', 'Message 7', 'api', '2026-03-09 11:00:01')");
        $this->mysqli->query("INSERT INTO mi_bi_log_entries VALUES (8, 'INFO', 'Message 8', 'api', '2026-03-09 11:00:02')");

        // Final state: 7 total (5 - 1 + 3)
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_bi_log_entries');
        $this->assertSame(7, (int) $rows[0]['cnt']);

        // ERROR: 2, INFO: 3 (4,5,8), WARN: 1 (6), DEBUG: 1 (7)
        $rows = $this->ztdQuery(
            "SELECT level, COUNT(*) AS cnt FROM mi_bi_log_entries GROUP BY level ORDER BY level"
        );
        $this->assertCount(4, $rows);
        $this->assertSame('DEBUG', $rows[0]['level']);
        $this->assertSame(1, (int) $rows[0]['cnt']);
        $this->assertSame('ERROR', $rows[1]['level']);
        $this->assertSame(2, (int) $rows[1]['cnt']);
        $this->assertSame('INFO', $rows[2]['level']);
        $this->assertSame(3, (int) $rows[2]['cnt']);
        $this->assertSame('WARN', $rows[3]['level']);
        $this->assertSame(1, (int) $rows[3]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testPhysicalIsolation(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->mysqli->query(
                "INSERT INTO mi_bi_log_entries VALUES ($i, 'INFO', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        // Visible through ZTD
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_bi_log_entries');
        $this->assertSame(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_bi_log_entries');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
