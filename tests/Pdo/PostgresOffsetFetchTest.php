<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SQL:2012 OFFSET...FETCH syntax on PostgreSQL ZTD.
 *
 * PostgreSQL supports both:
 *   LIMIT x OFFSET y (traditional)
 *   OFFSET y ROWS FETCH FIRST x ROWS ONLY (SQL:2012 standard)
 *
 * The CTE rewriter should preserve these clauses correctly.
 * @spec SPEC-3.1
 */
class PostgresOffsetFetchTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_of_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_of_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO pg_of_test (id, name, score) VALUES ($i, 'User_$i', " . ($i * 10) . ")");
        }
    }

    /**
     * Traditional LIMIT/OFFSET works.
     */
    public function testTraditionalLimitOffset(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM pg_of_test ORDER BY id LIMIT 3 OFFSET 2');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([3, 4, 5], array_map('intval', $rows));
    }

    /**
     * FETCH FIRST N ROWS ONLY (SQL:2012).
     */
    public function testFetchFirstNRowsOnly(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM pg_of_test ORDER BY id FETCH FIRST 3 ROWS ONLY');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([1, 2, 3], array_map('intval', $rows));
    }

    /**
     * OFFSET N ROWS FETCH FIRST M ROWS ONLY.
     */
    public function testOffsetRowsFetchFirst(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM pg_of_test ORDER BY id OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([3, 4, 5], array_map('intval', $rows));
    }

    /**
     * FETCH NEXT 1 ROW ONLY (single row).
     */
    public function testFetchNextOneRowOnly(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM pg_of_test ORDER BY id OFFSET 4 ROWS FETCH NEXT 1 ROW ONLY');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([5], array_map('intval', $rows));
    }

    /**
     * OFFSET without FETCH returns all remaining rows.
     */
    public function testOffsetWithoutFetch(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM pg_of_test ORDER BY id OFFSET 7');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([8, 9, 10], array_map('intval', $rows));
    }

    /**
     * OFFSET...FETCH with WHERE clause.
     */
    public function testOffsetFetchWithWhere(): void
    {
        $stmt = $this->pdo->query('SELECT id FROM pg_of_test WHERE score >= 50 ORDER BY id OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([6, 7], array_map('intval', $rows));
    }

    /**
     * Physical isolation: pagination queries still work after disabling ZTD.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_of_test');
        $this->assertSame(10, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_of_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
