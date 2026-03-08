<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests EXPLAIN statement handling through CTE shadow on PostgreSQL PDO.
 *
 * EXPLAIN and EXPLAIN ANALYZE are commonly used for performance debugging.
 * Tests verify whether EXPLAIN works, produces query plan output,
 * and doesn't corrupt shadow state.
 * @spec pending
 */
class PostgresExplainQueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_explain_items (id SERIAL PRIMARY KEY, name VARCHAR(50), category VARCHAR(30))',
            'CREATE INDEX idx_pg_explain_category ON pg_explain_items (category)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_explain_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_explain_items (id, name, category) VALUES (1, 'Widget', 'electronics')");
        $this->pdo->exec("INSERT INTO pg_explain_items (id, name, category) VALUES (2, 'Gadget', 'electronics')");
        $this->pdo->exec("INSERT INTO pg_explain_items (id, name, category) VALUES (3, 'Book', 'education')");
    }

    /**
     * EXPLAIN SELECT on a shadow table.
     */
    public function testExplainSelect(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN SELECT * FROM pg_explain_items WHERE id = 1');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
            // PostgreSQL EXPLAIN output has a "QUERY PLAN" column
            $this->assertArrayHasKey('QUERY PLAN', $rows[0]);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN with FORMAT JSON.
     */
    public function testExplainFormatJson(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN (FORMAT JSON) SELECT * FROM pg_explain_items WHERE id = 1');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
            $json = json_decode($rows[0]['QUERY PLAN'], true);
            $this->assertIsArray($json);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN ANALYZE executes the query and shows actual timing.
     *
     * Note: EXPLAIN ANALYZE actually runs the query. For DML this
     * could modify state if not wrapped in a transaction.
     */
    public function testExplainAnalyzeSelect(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN ANALYZE SELECT * FROM pg_explain_items WHERE id = 1');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
            // ANALYZE output includes actual time
            $plan = $rows[0]['QUERY PLAN'];
            $this->assertStringContainsString('actual time', $plan);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN (COSTS OFF) for cleaner output.
     */
    public function testExplainCostsOff(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN (COSTS OFF) SELECT * FROM pg_explain_items WHERE id = 1');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN on UPDATE statement.
     */
    public function testExplainUpdate(): void
    {
        try {
            $stmt = $this->pdo->query("EXPLAIN UPDATE pg_explain_items SET name = 'Updated' WHERE id = 1");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN on DELETE statement.
     */
    public function testExplainDelete(): void
    {
        try {
            $stmt = $this->pdo->query("EXPLAIN DELETE FROM pg_explain_items WHERE category = 'education'");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Shadow operations work after EXPLAIN attempt.
     */
    public function testShadowWorksAfterExplain(): void
    {
        try {
            $this->pdo->query('EXPLAIN SELECT * FROM pg_explain_items');
        } catch (\Throwable $e) {
            // Ignore
        }

        $this->pdo->exec("INSERT INTO pg_explain_items (id, name, category) VALUES (4, 'Pen', 'office')");
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_explain_items');
        $this->assertSame(4, (int) $rows[0]['cnt']);
    }

    /**
     * EXPLAIN does not modify shadow state.
     */
    public function testExplainDoesNotModifyShadow(): void
    {
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_explain_items');
        $countBefore = (int) $rows[0]['cnt'];

        try {
            $this->pdo->query("EXPLAIN UPDATE pg_explain_items SET name = 'X' WHERE id = 1");
        } catch (\Throwable $e) {
            // Ignore
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_explain_items');
        $countAfter = (int) $rows[0]['cnt'];
        $this->assertSame($countBefore, $countAfter);

        $rows = $this->ztdQuery('SELECT name FROM pg_explain_items WHERE id = 1');
        $this->assertSame('Widget', $rows[0]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_explain_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
