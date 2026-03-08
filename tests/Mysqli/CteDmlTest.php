<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CTE-based DML patterns on MySQLi ZTD.
 *
 * MySQL 8.0+ supports WITH ... INSERT/UPDATE/DELETE natively, but ZTD
 * does not support these patterns. The mutation resolver cannot produce
 * a shadow mutation for CTE-based DML.
 * @spec SPEC-3.3e
 */
class CteDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cte_dml_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE mi_cte_dml_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cte_dml_target', 'mi_cte_dml_source'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cte_dml_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_cte_dml_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_cte_dml_source (id, name, score) VALUES (3, 'Charlie', 70)");
        $this->mysqli->query("INSERT INTO mi_cte_dml_target (id, name, score) VALUES (1, 'Old_Alice', 50)");
        $this->mysqli->query("INSERT INTO mi_cte_dml_target (id, name, score) VALUES (2, 'Old_Bob', 40)");
    }

    /**
     * WITH ... INSERT fails on MySQLi ZTD.
     */
    public function testWithInsertSelectFails(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->mysqli->query("WITH high_scores AS (SELECT id, name, score FROM mi_cte_dml_source WHERE score >= 80) INSERT INTO mi_cte_dml_target (id, name, score) SELECT id + 10, name, score FROM high_scores");
    }

    /**
     * WITH ... DELETE fails on MySQLi ZTD.
     */
    public function testWithDeleteFails(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->mysqli->query("WITH low_scores AS (SELECT id FROM mi_cte_dml_target WHERE score < 45) DELETE FROM mi_cte_dml_target WHERE id IN (SELECT id FROM low_scores)");
    }

    /**
     * WITH ... UPDATE fails on MySQLi ZTD.
     */
    public function testWithUpdateFails(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->mysqli->query("WITH new_scores AS (SELECT id, score FROM mi_cte_dml_source WHERE id <= 2) UPDATE mi_cte_dml_target SET score = 100 WHERE id IN (SELECT id FROM new_scores)");
    }

    /**
     * Shadow store is not corrupted by CTE DML failures.
     */
    public function testShadowStoreIntactAfterCteDmlFailure(): void
    {
        try {
            $this->mysqli->query("WITH hs AS (SELECT id FROM mi_cte_dml_source) INSERT INTO mi_cte_dml_target (id, name, score) SELECT id + 10, 'x', 0 FROM hs");
        } catch (\Throwable $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cte_dml_target');
        $this->assertEquals(2, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cte_dml_source');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation: seed data is only in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cte_dml_target');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
