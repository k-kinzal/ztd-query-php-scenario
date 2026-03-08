<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PostgreSQL array type handling with ZTD.
 *
 * PostgreSQL supports array columns (INTEGER[], TEXT[], etc.).
 * Schema reflection recognizes array types via resolveArrayType(),
 * but the CTE rewriter does NOT properly handle array types:
 *
 * - INSERT with array values succeeds (InsertTransformer passes values through).
 * - SELECT after INSERT fails because the CTE rewriter embeds stored array
 *   literals with CAST as the base type (e.g., CAST('{90,85}' AS INTEGER))
 *   instead of the array type (INTEGER[]).
 * - TEXT[] columns work because CAST to TEXT is compatible.
 * - NULL array values work correctly.
 * - ARRAY[] constructor syntax causes InsertTransformer to mismatch column count.
 */
class PostgresArrayTypeTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_arr_test');
        $raw->exec('CREATE TABLE pg_arr_test (id INT PRIMARY KEY, name VARCHAR(50), tags TEXT[], scores INTEGER[])');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');
    }

    /**
     * INSERT with curly brace array literal succeeds, but SELECT fails.
     *
     * The INSERT passes through the InsertTransformer, but subsequent SELECT
     * generates CAST('{90,85,92}' AS INTEGER) instead of CAST(...AS INTEGER[]),
     * causing "invalid input syntax for type integer".
     */
    public function testInsertSucceedsButSelectFailsForIntegerArray(): void
    {
        // INSERT succeeds
        $this->pdo->exec("INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (1, 'Alice', '{\"php\",\"sql\"}', '{90,85,92}')");

        // SELECT fails due to invalid CAST for INTEGER[] column
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('invalid input syntax for type integer');
        $this->pdo->query('SELECT tags, scores FROM pg_arr_test WHERE id = 1');
    }

    /**
     * INSERT with ARRAY[] constructor fails at InsertTransformer level.
     *
     * The InsertTransformer misparses ARRAY['go','rust'] as multiple
     * VALUES entries, causing "values count does not match column count".
     */
    public function testInsertWithArrayConstructorFails(): void
    {
        $this->expectException(\Exception::class);
        $this->pdo->exec("INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (2, 'Bob', ARRAY['go','rust'], ARRAY[80,75])");
    }

    /**
     * INSERT with empty array also triggers CAST issue on SELECT.
     */
    public function testInsertEmptyArraySucceedsButSelectFails(): void
    {
        $this->pdo->exec("INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (3, 'Charlie', '{}', '{}')");

        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('invalid input syntax for type integer');
        $this->pdo->query('SELECT scores FROM pg_arr_test WHERE id = 3');
    }

    /**
     * INSERT with NULL array values works correctly.
     *
     * NULL is type-agnostic and bypasses the CAST issue.
     */
    public function testInsertWithNullArraySucceeds(): void
    {
        $this->pdo->exec("INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (4, 'Diana', NULL, NULL)");

        $stmt = $this->pdo->query('SELECT tags, scores FROM pg_arr_test WHERE id = 4');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['tags']);
        $this->assertNull($row['scores']);
    }

    /**
     * Prepared INSERT with array param succeeds but SELECT fails.
     */
    public function testPreparedInsertSucceedsButSelectFails(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (?, ?, ?, ?)');
        $stmt->execute([5, 'Eve', '{python,java}', '{95,88}']);

        // SELECT with scores column fails due to CAST issue
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('invalid input syntax for type integer');
        $this->pdo->query('SELECT scores FROM pg_arr_test WHERE id = 5');
    }

    /**
     * TEXT[] column works because CAST to TEXT is compatible.
     *
     * When only selecting TEXT[] columns (not INTEGER[]), the SELECT succeeds.
     */
    public function testTextArrayColumnWorks(): void
    {
        $this->pdo->exec("INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (6, 'Frank', '{\"php\",\"go\"}', NULL)");

        // SELECT only TEXT[] column — CAST to TEXT is compatible
        $stmt = $this->pdo->query('SELECT name, tags FROM pg_arr_test WHERE id = 6');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Frank', $row['name']);
        $this->assertNotNull($row['tags']);
    }

    /**
     * Even selecting non-array columns fails if table has INTEGER[] columns.
     *
     * The CTE rewriter builds the shadow CTE with ALL columns of the table,
     * so the INTEGER[] CAST issue affects any query on the table.
     */
    public function testSelectNonArrayColumnsAlsoFails(): void
    {
        $this->pdo->exec("INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (7, 'Grace', '{\"js\"}', '{100}')");

        // Even though we only SELECT id, name — the CTE includes scores column
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('invalid input syntax for type integer');
        $this->pdo->query('SELECT id, name FROM pg_arr_test WHERE id = 7');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_arr_test (id, name, tags, scores) VALUES (8, 'Test', NULL, NULL)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_arr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_arr_test');
        } catch (\Exception $e) {
        }
    }
}
