<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CTE-based DML patterns on PostgreSQL PDO ZTD.
 *
 * PostgreSQL supports WITH ... INSERT/UPDATE/DELETE natively, but ZTD
 * does not support these patterns. The CTE rewriter produces invalid SQL
 * because it does not properly handle user CTEs combined with DML statements.
 * @spec SPEC-3.3e
 */
class PostgresCteDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cte_dml_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE pg_cte_dml_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cte_dml_target', 'pg_cte_dml_source'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cte_dml_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_cte_dml_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pg_cte_dml_source (id, name, score) VALUES (3, 'Charlie', 70)");
        $this->pdo->exec("INSERT INTO pg_cte_dml_target (id, name, score) VALUES (1, 'Old_Alice', 50)");
        $this->pdo->exec("INSERT INTO pg_cte_dml_target (id, name, score) VALUES (2, 'Old_Bob', 40)");
    }

    /**
     * WITH ... INSERT fails on PostgreSQL ZTD.
     *
     * The CTE rewriter produces invalid SQL — it wraps the WITH clause
     * in a way that creates a syntax error.
     */
    public function testWithInsertSelectFails(): void
    {
        $this->expectException(\Throwable::class);

        $this->pdo->exec("WITH high_scores AS (SELECT id, name, score FROM pg_cte_dml_source WHERE score >= 80) INSERT INTO pg_cte_dml_target (id, name, score) SELECT id + 10, name, score FROM high_scores");
    }

    /**
     * WITH ... DELETE fails on PostgreSQL ZTD.
     *
     * Table references inside the user CTE are not rewritten, so
     * the query references the physical table (which has no data).
     */
    public function testWithDeleteFails(): void
    {
        $this->expectException(\Throwable::class);

        $this->pdo->exec("WITH low_scores AS (SELECT id FROM pg_cte_dml_target WHERE score < 45) DELETE FROM pg_cte_dml_target WHERE id IN (SELECT id FROM low_scores)");
    }

    /**
     * WITH ... UPDATE fails on PostgreSQL ZTD.
     */
    public function testWithUpdateFails(): void
    {
        $this->expectException(\Throwable::class);

        $this->pdo->exec("WITH new_scores AS (SELECT id, score FROM pg_cte_dml_source WHERE id <= 2) UPDATE pg_cte_dml_target SET score = 100 WHERE id IN (SELECT id FROM new_scores)");
    }

    /**
     * Shadow store is not corrupted by CTE DML failures.
     */
    public function testShadowStoreIntactAfterCteDmlFailure(): void
    {
        try {
            $this->pdo->exec("WITH hs AS (SELECT id FROM pg_cte_dml_source) INSERT INTO pg_cte_dml_target (id, name, score) SELECT id + 10, 'x', 0 FROM hs");
        } catch (\Throwable $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_cte_dml_target');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_cte_dml_source');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation: seed data is only in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_cte_dml_target');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
