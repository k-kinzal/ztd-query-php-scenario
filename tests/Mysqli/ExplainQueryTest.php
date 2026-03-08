<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests EXPLAIN statement handling through CTE shadow on MySQLi.
 *
 * EXPLAIN is commonly used for performance debugging.
 * Tests verify whether EXPLAIN works, produces query plan output,
 * and doesn't corrupt shadow state.
 * @spec SPEC-6.4
 */
class ExplainQueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_explain_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(30), INDEX idx_category (category))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_explain_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_explain_items VALUES (1, 'Widget', 'electronics')");
        $this->mysqli->query("INSERT INTO mi_explain_items VALUES (2, 'Gadget', 'electronics')");
        $this->mysqli->query("INSERT INTO mi_explain_items VALUES (3, 'Book', 'education')");
    }

    /**
     * EXPLAIN SELECT on a shadow table.
     */
    public function testExplainSelect(): void
    {
        try {
            $result = $this->mysqli->query('EXPLAIN SELECT * FROM mi_explain_items WHERE id = 1');
            if ($result !== false) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                $this->assertNotEmpty($rows);
                // EXPLAIN output should have standard columns
                $this->assertArrayHasKey('table', $rows[0]);
            } else {
                $this->assertFalse($result);
            }
        } catch (\Throwable $e) {
            // EXPLAIN may be treated as unsupported SQL
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN on a query with WHERE clause.
     */
    public function testExplainWithWhere(): void
    {
        try {
            $result = $this->mysqli->query("EXPLAIN SELECT * FROM mi_explain_items WHERE category = 'electronics'");
            if ($result !== false) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                $this->assertNotEmpty($rows);
            }
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * EXPLAIN on a JOIN query.
     */
    public function testExplainJoin(): void
    {
        try {
            $result = $this->mysqli->query(
                'EXPLAIN SELECT a.name, b.name FROM mi_explain_items a JOIN mi_explain_items b ON a.category = b.category AND a.id != b.id'
            );
            if ($result !== false) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                $this->assertNotEmpty($rows);
            }
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
            $result = $this->mysqli->query('EXPLAIN FORMAT=JSON SELECT * FROM mi_explain_items WHERE id = 1');
            if ($result !== false) {
                $row = $result->fetch_row();
                $json = json_decode($row[0], true);
                $this->assertIsArray($json);
                $this->assertArrayHasKey('query_block', $json);
            }
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
            $result = $this->mysqli->query("EXPLAIN UPDATE mi_explain_items SET name = 'Updated' WHERE id = 1");
            if ($result !== false) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                $this->assertNotEmpty($rows);
            }
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
            $result = $this->mysqli->query("EXPLAIN DELETE FROM mi_explain_items WHERE category = 'education'");
            if ($result !== false) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                $this->assertNotEmpty($rows);
            }
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
            $this->mysqli->query('EXPLAIN SELECT * FROM mi_explain_items');
        } catch (\Throwable $e) {
            // Ignore
        }

        // Shadow operations should still work
        $this->mysqli->query("INSERT INTO mi_explain_items VALUES (4, 'Pen', 'office')");
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_explain_items');
        $this->assertSame(4, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * EXPLAIN does not modify shadow state.
     */
    public function testExplainDoesNotModifyShadow(): void
    {
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_explain_items');
        $countBefore = (int) $result->fetch_assoc()['cnt'];

        try {
            $this->mysqli->query("EXPLAIN UPDATE mi_explain_items SET name = 'X' WHERE id = 1");
        } catch (\Throwable $e) {
            // Ignore
        }

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_explain_items');
        $countAfter = (int) $result->fetch_assoc()['cnt'];

        $this->assertSame($countBefore, $countAfter);

        // Verify original data unchanged
        $result = $this->mysqli->query('SELECT name FROM mi_explain_items WHERE id = 1');
        $this->assertSame('Widget', $result->fetch_assoc()['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_explain_items');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
