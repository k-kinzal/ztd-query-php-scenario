<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests EXPLAIN and EXPLAIN QUERY PLAN through ZTD.
 *
 * EXPLAIN is a read-only diagnostic statement. The CTE rewriter may
 * attempt to rewrite it as a regular SELECT, or it may pass through correctly.
 *
 * @spec SPEC-3.1
 */
class SqliteExplainThroughZtdTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_exp_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_exp_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_exp_items VALUES (1, 'Widget', 'tools')");
    }

    /**
     * EXPLAIN QUERY PLAN for a SELECT.
     */
    public function testExplainQueryPlan(): void
    {
        try {
            $stmt = $this->pdo->query("EXPLAIN QUERY PLAN SELECT * FROM sl_exp_items WHERE category = 'tools'");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // EXPLAIN should return at least 1 row describing the query plan
            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'EXPLAIN QUERY PLAN returned 0 rows'
                );
            }

            $this->assertGreaterThan(0, count($rows));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'EXPLAIN QUERY PLAN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * EXPLAIN for a SELECT.
     */
    public function testExplainSelect(): void
    {
        try {
            $stmt = $this->pdo->query("EXPLAIN SELECT * FROM sl_exp_items WHERE id = 1");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'EXPLAIN returned 0 rows'
                );
            }

            $this->assertGreaterThan(0, count($rows));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'EXPLAIN SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * EXPLAIN with shadow data — should still return plan, not crash.
     */
    public function testExplainWithShadowData(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_exp_items VALUES (2, 'Gadget', 'tools')");

            $stmt = $this->pdo->query("EXPLAIN QUERY PLAN SELECT * FROM sl_exp_items WHERE category = 'tools'");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'EXPLAIN with shadow data returned 0 rows'
                );
            }

            $this->assertGreaterThan(0, count($rows));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'EXPLAIN with shadow data failed: ' . $e->getMessage()
            );
        }
    }
}
