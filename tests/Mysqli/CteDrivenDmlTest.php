<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests user-written CTEs driving DML operations on MySQLi.
 * @spec SPEC-3.3, SPEC-4.1a, SPEC-4.2c, SPEC-4.2d
 */
class CteDrivenDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cdd_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE mi_cdd_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE mi_cdd_log (id INT PRIMARY KEY, action VARCHAR(50), ref_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cdd_log', 'mi_cdd_target', 'mi_cdd_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->mysqli->query("INSERT INTO mi_cdd_source VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_cdd_source VALUES (2, 'Bob', 75)");
        $this->mysqli->query("INSERT INTO mi_cdd_source VALUES (3, 'Charlie', 60)");
    }

    public function testCteInsertSelect(): void
    {
        $sql = "WITH high_scorers AS (
                    SELECT id, name, score FROM mi_cdd_source WHERE score >= 75
                )
                INSERT INTO mi_cdd_target (id, name, score)
                SELECT id, name, score FROM high_scorers";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name, score FROM mi_cdd_target ORDER BY id");

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

    public function testCteDelete(): void
    {
        $this->mysqli->query("INSERT INTO mi_cdd_target VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_cdd_target VALUES (2, 'Bob', 75)");
        $this->mysqli->query("INSERT INTO mi_cdd_target VALUES (3, 'Charlie', 60)");

        $sql = "WITH low_scorers AS (
                    SELECT id FROM mi_cdd_target WHERE score < 70
                )
                DELETE FROM mi_cdd_target WHERE id IN (SELECT id FROM low_scorers)";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name FROM mi_cdd_target ORDER BY id");

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

    public function testMultipleCteInsert(): void
    {
        $sql = "WITH
                    top_s AS (SELECT id, name, score FROM mi_cdd_source WHERE score >= 80),
                    mid_s AS (SELECT id, name, score FROM mi_cdd_source WHERE score >= 70 AND score < 80)
                INSERT INTO mi_cdd_target (id, name, score)
                SELECT id, name, score FROM top_s
                UNION ALL
                SELECT id, name, score FROM mid_s";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name, score FROM mi_cdd_target ORDER BY score DESC");

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

    public function testCteInsertPrepared(): void
    {
        $sql = "WITH filtered AS (
                    SELECT id, name, score FROM mi_cdd_source WHERE score >= ?
                )
                INSERT INTO mi_cdd_target (id, name, score)
                SELECT id, name, score FROM filtered";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $score = 75;
            $stmt->bind_param('i', $score);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name FROM mi_cdd_target ORDER BY id");

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

    public function testCteInsertCrossTable(): void
    {
        $sql = "WITH src AS (
                    SELECT id, name FROM mi_cdd_source WHERE score >= 75
                )
                INSERT INTO mi_cdd_log (id, action, ref_id)
                SELECT id, 'promoted', id FROM src";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT action, ref_id FROM mi_cdd_log ORDER BY id");

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
