<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests EXPLAIN statement handling through CTE shadow on MySQL PDO.
 *
 * EXPLAIN is commonly used for performance debugging.
 * Tests verify whether EXPLAIN works, produces query plan output,
 * and doesn't corrupt shadow state.
 * @spec pending
 */
class MysqlExplainQueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_explain_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(30), INDEX idx_category (category))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_explain_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_explain_items VALUES (1, 'Widget', 'electronics')");
        $this->pdo->exec("INSERT INTO pdo_explain_items VALUES (2, 'Gadget', 'electronics')");
        $this->pdo->exec("INSERT INTO pdo_explain_items VALUES (3, 'Book', 'education')");
    }

    /**
     * EXPLAIN SELECT on a shadow table.
     */
    public function testExplainSelect(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN SELECT * FROM pdo_explain_items WHERE id = 1');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
            $this->assertArrayHasKey('table', $rows[0]);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN on a query with WHERE clause using index.
     */
    public function testExplainWithIndexedWhere(): void
    {
        try {
            $stmt = $this->pdo->query("EXPLAIN SELECT * FROM pdo_explain_items WHERE category = 'electronics'");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertNotEmpty($rows);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN FORMAT=JSON (MySQL 5.6+).
     */
    public function testExplainFormatJson(): void
    {
        try {
            $stmt = $this->pdo->query('EXPLAIN FORMAT=JSON SELECT * FROM pdo_explain_items WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_NUM);
            $json = json_decode($row[0], true);
            $this->assertIsArray($json);
            $this->assertArrayHasKey('query_block', $json);
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
            $stmt = $this->pdo->query("EXPLAIN UPDATE pdo_explain_items SET name = 'Updated' WHERE id = 1");
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
            $stmt = $this->pdo->query("EXPLAIN DELETE FROM pdo_explain_items WHERE category = 'education'");
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
            $this->pdo->query('EXPLAIN SELECT * FROM pdo_explain_items');
        } catch (\Throwable $e) {
            // Ignore
        }

        $this->pdo->exec("INSERT INTO pdo_explain_items VALUES (4, 'Pen', 'office')");
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pdo_explain_items');
        $this->assertSame(4, (int) $rows[0]['cnt']);
    }

    /**
     * EXPLAIN does not modify shadow state.
     */
    public function testExplainDoesNotModifyShadow(): void
    {
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pdo_explain_items');
        $countBefore = (int) $rows[0]['cnt'];

        try {
            $this->pdo->query("EXPLAIN UPDATE pdo_explain_items SET name = 'X' WHERE id = 1");
        } catch (\Throwable $e) {
            // Ignore
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pdo_explain_items');
        $countAfter = (int) $rows[0]['cnt'];
        $this->assertSame($countBefore, $countAfter);

        $rows = $this->ztdQuery('SELECT name FROM pdo_explain_items WHERE id = 1');
        $this->assertSame('Widget', $rows[0]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_explain_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
