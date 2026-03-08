<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CREATE TABLE AS SELECT (CTAS) on PostgreSQL PDO.
 *
 * CTAS creates a new shadow table and populates it. However:
 * - Column types default to TEXT (no schema reflection), so type comparisons
 *   may require explicit casting.
 * - CTAS with empty result set (WHERE 1=0) throws "Cannot determine columns"
 *   because column inference requires at least one row.
 * @spec SPEC-5.1c
 */
class PostgresCtasTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ctas_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE pg_ctas_copy AS SELECT * FROM pg_ctas_source',
            'CREATE TABLE pg_ctas_filtered AS SELECT * FROM pg_ctas_source WHERE score >= 85',
            'CREATE TABLE pg_ctas_empty AS SELECT * FROM pg_ctas_source WHERE 1=0',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ctas_copy', 'pg_ctas_filtered', 'pg_ctas_source', 'AS', 'pg_ctas_empty'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ctas_source VALUES (1, 'Alice', 95)");
        $this->pdo->exec("INSERT INTO pg_ctas_source VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO pg_ctas_source VALUES (3, 'Charlie', 75)");
    }

    /**
     * CTAS copies shadow data. Column types default to TEXT,
     * requiring string comparison (not integer).
     */
    public function testCtasCopiesData(): void
    {
        $this->pdo->exec('CREATE TABLE pg_ctas_copy AS SELECT * FROM pg_ctas_source');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctas_copy');
        $this->assertEquals(3, (int) $stmt->fetchColumn());

        // Column types are TEXT in shadow, so use string comparison
        $stmt = $this->pdo->query("SELECT name FROM pg_ctas_copy WHERE id = '1'");
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * CTAS with WHERE filter.
     */
    public function testCtasWithFilter(): void
    {
        $this->pdo->exec("CREATE TABLE pg_ctas_filtered AS SELECT * FROM pg_ctas_source WHERE score >= 85");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctas_filtered');
        $this->assertEquals(2, (int) $stmt->fetchColumn());
    }

    /**
     * CTAS with empty result throws — can't determine columns from 0 rows.
     */
    public function testCtasEmptyResultThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot determine columns');
        $this->pdo->exec('CREATE TABLE pg_ctas_empty AS SELECT * FROM pg_ctas_source WHERE 1=0');
    }

    /**
     * CTAS column types default to TEXT — integer comparison fails.
     */
    public function testCtasColumnTypesDefaultToText(): void
    {
        $this->pdo->exec('CREATE TABLE pg_ctas_copy AS SELECT * FROM pg_ctas_source');

        // Integer comparison fails because column is TEXT
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT name FROM pg_ctas_copy WHERE id = 1');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctas_source');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
