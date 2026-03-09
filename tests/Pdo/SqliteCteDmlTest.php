<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CTE-based DML patterns on SQLite PDO ZTD.
 *
 * SQLite supports WITH ... INSERT/UPDATE/DELETE natively, but ZTD
 * does not support these patterns. The CTE rewriter prepends its own
 * shadow CTE, which prevents user CTE names from being visible in
 * the DML statement, causing "no such table" errors.
 * @spec SPEC-3.3e
 * @see https://github.com/k-kinzal/ztd-query-php/issues/28
 */
class SqliteCteDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE cte_dml_source (id INTEGER PRIMARY KEY, name TEXT, score INT)',
            'CREATE TABLE cte_dml_target (id INTEGER PRIMARY KEY, name TEXT, score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['cte_dml_source', 'cte_dml_target'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO cte_dml_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO cte_dml_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO cte_dml_source (id, name, score) VALUES (3, 'Charlie', 70)");
        $this->pdo->exec("INSERT INTO cte_dml_target (id, name, score) VALUES (1, 'Old_Alice', 50)");
        $this->pdo->exec("INSERT INTO cte_dml_target (id, name, score) VALUES (2, 'Old_Bob', 40)");
    }
    /**
     * WITH ... INSERT fails because user CTE is not visible after ZTD rewriting.
     *
     * The ZTD CTE rewriter prepends shadow CTEs, causing the user's CTE
     * name to be lost. The INSERT references the CTE name which no longer exists.
     */
    public function testWithInsertSelectFails(): void
    {
        $this->expectException(\Throwable::class);

        $this->pdo->exec("WITH high_scores AS (SELECT id, name, score FROM cte_dml_source WHERE score >= 80) INSERT INTO cte_dml_target (id, name, score) SELECT id + 10, name, score FROM high_scores");
    }

    /**
     * WITH ... DELETE fails on SQLite ZTD.
     */
    public function testWithDeleteFails(): void
    {
        $this->expectException(\Throwable::class);

        $this->pdo->exec("WITH low_scores AS (SELECT id FROM cte_dml_target WHERE score < 45) DELETE FROM cte_dml_target WHERE id IN (SELECT id FROM low_scores)");
    }

    /**
     * WITH ... UPDATE fails on SQLite ZTD.
     */
    public function testWithUpdateFails(): void
    {
        $this->expectException(\Throwable::class);

        $this->pdo->exec("WITH new_scores AS (SELECT id, score FROM cte_dml_source WHERE id <= 2) UPDATE cte_dml_target SET score = (SELECT ns.score FROM new_scores ns WHERE ns.id = cte_dml_target.id) WHERE id IN (SELECT id FROM new_scores)");
    }

    /**
     * Shadow store is not corrupted by CTE DML failures.
     */
    public function testShadowStoreIntactAfterCteDmlFailure(): void
    {
        try {
            $this->pdo->exec("WITH hs AS (SELECT id FROM cte_dml_source) INSERT INTO cte_dml_target (id, name, score) SELECT id + 10, 'x', 0 FROM hs");
        } catch (\Throwable $e) {
            // Expected
        }

        // Shadow store should still be intact
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM cte_dml_target');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM cte_dml_source');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }
}
