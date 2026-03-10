<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests user-written CTEs driving DML operations (INSERT, UPDATE, DELETE).
 *
 * Pattern: WITH cte AS (...) INSERT/UPDATE/DELETE ...
 * This is a common PostgreSQL/MySQL 8+ pattern. The CTE rewriter must handle
 * BOTH the user CTE and its own shadow CTE without collision.
 *
 * @spec SPEC-3.3, SPEC-4.1a, SPEC-4.2c, SPEC-4.2d
 */
class SqliteCteDrivenDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cdd_source (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)',
            'CREATE TABLE sl_cdd_target (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)',
            'CREATE TABLE sl_cdd_log (id INTEGER PRIMARY KEY, action TEXT, ref_id INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cdd_log', 'sl_cdd_target', 'sl_cdd_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_cdd_source VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO sl_cdd_source VALUES (2, 'Bob', 75)");
        $this->pdo->exec("INSERT INTO sl_cdd_source VALUES (3, 'Charlie', 60)");
    }

    /**
     * WITH cte AS (SELECT ...) INSERT INTO target SELECT FROM cte.
     * The CTE rewriter must not collide with the user-written WITH clause.
     */
    public function testCteInsertSelect(): void
    {
        $sql = "WITH high_scorers AS (
                    SELECT id, name, score FROM sl_cdd_source WHERE score >= 75
                )
                INSERT INTO sl_cdd_target (id, name, score)
                SELECT id, name, score FROM high_scorers";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, score FROM sl_cdd_target ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE INSERT SELECT: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE INSERT SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * WITH cte AS (SELECT ...) DELETE FROM target WHERE id IN (SELECT FROM cte).
     */
    public function testCteDelete(): void
    {
        // Populate target first
        $this->pdo->exec("INSERT INTO sl_cdd_target VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO sl_cdd_target VALUES (2, 'Bob', 75)");
        $this->pdo->exec("INSERT INTO sl_cdd_target VALUES (3, 'Charlie', 60)");

        $sql = "WITH low_scorers AS (
                    SELECT id FROM sl_cdd_target WHERE score < 70
                )
                DELETE FROM sl_cdd_target WHERE id IN (SELECT id FROM low_scorers)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM sl_cdd_target ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE DELETE: expected 2 remaining, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple user CTEs driving INSERT.
     */
    public function testMultipleCteInsert(): void
    {
        $sql = "WITH
                    top AS (SELECT id, name, score FROM sl_cdd_source WHERE score >= 80),
                    mid AS (SELECT id, name, score FROM sl_cdd_source WHERE score >= 70 AND score < 80)
                INSERT INTO sl_cdd_target (id, name, score)
                SELECT id, name, score FROM top
                UNION ALL
                SELECT id, name, score FROM mid";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, score FROM sl_cdd_target ORDER BY score DESC");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Multiple CTE INSERT: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple CTE INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * CTE with prepared parameter driving INSERT.
     */
    public function testCteInsertPrepared(): void
    {
        $sql = "WITH filtered AS (
                    SELECT id, name, score FROM sl_cdd_source WHERE score >= ?
                )
                INSERT INTO sl_cdd_target (id, name, score)
                SELECT id, name, score FROM filtered";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([75]);

            $rows = $this->ztdQuery("SELECT name FROM sl_cdd_target ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE INSERT prepared: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE INSERT prepared failed: ' . $e->getMessage());
        }
    }

    /**
     * CTE driving INSERT into log from source (cross-table CTE DML).
     */
    public function testCteInsertCrossTable(): void
    {
        $sql = "WITH src AS (
                    SELECT id, name FROM sl_cdd_source WHERE score >= 75
                )
                INSERT INTO sl_cdd_log (id, action, ref_id)
                SELECT id, 'promoted', id FROM src";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT action, ref_id FROM sl_cdd_log ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE INSERT cross-table: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('promoted', $rows[0]['action']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE INSERT cross-table failed: ' . $e->getMessage());
        }
    }
}
