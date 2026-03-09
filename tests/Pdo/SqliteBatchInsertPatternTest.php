<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multiple sequential INSERTs, batch insert visibility, and large dataset
 * insertion then aggregation through the ZTD shadow store (SQLite PDO).
 * Empty tables at start - tests INSERT-then-read pattern from scratch.
 * @spec SPEC-10.2.94
 */
class SqliteBatchInsertPatternTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_bi_log_entries (
            id INTEGER PRIMARY KEY,
            level TEXT,
            message TEXT,
            source TEXT,
            created_at TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_bi_log_entries'];
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
            $this->pdo->exec(
                "INSERT INTO sl_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:0$i')"
            );
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_bi_log_entries');
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
            $this->pdo->exec(
                "INSERT INTO sl_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_bi_log_entries WHERE level = 'ERROR'");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_bi_log_entries WHERE level = 'INFO'");
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
            $this->pdo->exec(
                "INSERT INTO sl_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        $rows = $this->ztdQuery(
            "SELECT level, COUNT(*) AS cnt FROM sl_bi_log_entries GROUP BY level ORDER BY level"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('DEBUG', $rows[0]['level']);
        $this->assertSame(3, (int) $rows[0]['cnt']);
        $this->assertSame('ERROR', $rows[1]['level']);
        $this->assertSame(3, (int) $rows[1]['cnt']);
        $this->assertSame('INFO', $rows[2]['level']);
        $this->assertSame(4, (int) $rows[2]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testInsertUpdateRead(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->pdo->exec(
                "INSERT INTO sl_bi_log_entries VALUES ($i, 'INFO', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        $affected = $this->pdo->exec("UPDATE sl_bi_log_entries SET level = 'ERROR' WHERE id <= 2");
        $this->assertSame(2, $affected);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_bi_log_entries WHERE level = 'ERROR'");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_bi_log_entries WHERE level = 'INFO'");
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
            $this->pdo->exec(
                "INSERT INTO sl_bi_log_entries VALUES ($i, '$level', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        $affected = $this->pdo->exec("DELETE FROM sl_bi_log_entries WHERE level = 'DEBUG'");
        $this->assertSame(3, $affected);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_bi_log_entries');
        $this->assertSame(7, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_bi_log_entries WHERE level = 'DEBUG'");
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }

    /**
     * @spec SPEC-10.2.94
     */
    public function testMixedDmlVisibility(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->pdo->exec(
                "INSERT INTO sl_bi_log_entries VALUES ($i, 'INFO', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        $this->pdo->exec("UPDATE sl_bi_log_entries SET level = 'ERROR' WHERE id IN (1, 2)");
        $this->pdo->exec("DELETE FROM sl_bi_log_entries WHERE id = 3");
        $this->pdo->exec("INSERT INTO sl_bi_log_entries VALUES (6, 'WARN', 'Message 6', 'api', '2026-03-09 11:00:00')");
        $this->pdo->exec("INSERT INTO sl_bi_log_entries VALUES (7, 'DEBUG', 'Message 7', 'api', '2026-03-09 11:00:01')");
        $this->pdo->exec("INSERT INTO sl_bi_log_entries VALUES (8, 'INFO', 'Message 8', 'api', '2026-03-09 11:00:02')");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_bi_log_entries');
        $this->assertSame(7, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery(
            "SELECT level, COUNT(*) AS cnt FROM sl_bi_log_entries GROUP BY level ORDER BY level"
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
            $this->pdo->exec(
                "INSERT INTO sl_bi_log_entries VALUES ($i, 'INFO', 'Message $i', 'app', '2026-03-09 10:00:00')"
            );
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_bi_log_entries');
        $this->assertSame(5, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_bi_log_entries')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
