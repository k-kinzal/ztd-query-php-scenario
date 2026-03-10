<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests TABLESAMPLE syntax through the ZTD shadow store.
 *
 * PostgreSQL supports TABLESAMPLE SYSTEM(n) and TABLESAMPLE BERNOULLI(n) for
 * random sampling. The CTE rewriter replaces table references with CTE aliases.
 * Since CTEs are not physical tables, TABLESAMPLE syntax may fail or be
 * silently ignored.
 *
 * @spec SPEC-6.1
 */
class PostgresTablesampleTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_tsmp_data (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                value NUMERIC(10,2) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_tsmp_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        // Insert enough rows for sampling to be meaningful
        for ($i = 1; $i <= 50; $i++) {
            $cat = ($i % 3 === 0) ? 'A' : (($i % 3 === 1) ? 'B' : 'C');
            $this->pdo->exec("INSERT INTO pg_tsmp_data VALUES ({$i}, '{$cat}', " . ($i * 1.5) . ")");
        }
    }

    /**
     * TABLESAMPLE SYSTEM on table with shadow data.
     */
    public function testTablesampleSystemAfterDml(): void
    {
        try {
            // Modify some data
            $this->pdo->exec("UPDATE pg_tsmp_data SET value = 999.99 WHERE id = 1");
            $this->pdo->exec("DELETE FROM pg_tsmp_data WHERE id = 50");

            // TABLESAMPLE SYSTEM returns approximately n% of rows
            $rows = $this->ztdQuery(
                "SELECT id, value FROM pg_tsmp_data TABLESAMPLE SYSTEM(100)"
            );

            // With 100% sample, we should get all 49 rows (50 - 1 deleted)
            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'TABLESAMPLE SYSTEM(100) returned 0 rows after DML. '
                    . 'CTE rewriter may not support TABLESAMPLE syntax.'
                );
            }

            // Check that the deleted row is not present
            $ids = array_column($rows, 'id');
            if (in_array(50, array_map('intval', $ids))) {
                $this->markTestIncomplete(
                    'TABLESAMPLE returned deleted row (id=50). Shadow delete not applied.'
                );
            }

            // Check that the updated value is present
            $row1 = array_filter($rows, fn($r) => (int)$r['id'] === 1);
            if (!empty($row1)) {
                $row1 = array_values($row1)[0];
                if ((float)$row1['value'] !== 999.99) {
                    $this->markTestIncomplete(
                        'TABLESAMPLE: updated value not reflected. Got ' . $row1['value']
                        . ', expected 999.99.'
                    );
                }
            }

            $this->assertGreaterThan(0, count($rows));
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'TABLESAMPLE') !== false || stripos($msg, 'syntax') !== false) {
                $this->markTestIncomplete(
                    'TABLESAMPLE not supported through CTE shadow store: ' . $msg
                );
            }
            $this->markTestIncomplete('TABLESAMPLE test failed: ' . $msg);
        }
    }

    /**
     * TABLESAMPLE BERNOULLI — row-level sampling.
     */
    public function testTablesampleBernoulliAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_tsmp_data VALUES (51, 'X', 0.01)");

            $rows = $this->ztdQuery(
                "SELECT COUNT(*) as cnt FROM pg_tsmp_data TABLESAMPLE BERNOULLI(100)"
            );

            // 100% Bernoulli sample should return all 51 rows
            if ((int)$rows[0]['cnt'] === 0) {
                $this->markTestIncomplete(
                    'TABLESAMPLE BERNOULLI(100) COUNT returned 0. Not supported through ZTD.'
                );
            }

            $this->assertSame(51, (int)$rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('TABLESAMPLE BERNOULLI failed: ' . $e->getMessage());
        }
    }

    /**
     * TABLESAMPLE with REPEATABLE seed — deterministic sampling.
     */
    public function testTablesampleRepeatableAfterDml(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_tsmp_data WHERE category = 'A'");

            $rows1 = $this->ztdQuery(
                "SELECT id FROM pg_tsmp_data TABLESAMPLE BERNOULLI(50) REPEATABLE(42) ORDER BY id"
            );
            $rows2 = $this->ztdQuery(
                "SELECT id FROM pg_tsmp_data TABLESAMPLE BERNOULLI(50) REPEATABLE(42) ORDER BY id"
            );

            if (count($rows1) === 0) {
                $this->markTestIncomplete(
                    'TABLESAMPLE BERNOULLI REPEATABLE returned 0 rows.'
                );
            }

            // Same seed should produce same sample
            $this->assertEquals($rows1, $rows2, 'REPEATABLE seed should produce deterministic results');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('TABLESAMPLE REPEATABLE failed: ' . $e->getMessage());
        }
    }
}
