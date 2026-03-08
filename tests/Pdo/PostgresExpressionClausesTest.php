<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests expression-based clauses in shadow queries on PostgreSQL:
 * - ORDER BY with expressions
 * - GROUP BY with expressions
 * - HAVING with multiple conditions
 * - LIKE / ILIKE with ESCAPE clause
 * @spec pending
 */
class PostgresExpressionClausesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_expr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, bonus INT, category VARCHAR(10), search_term VARCHAR(100))';
    }

    protected function getTableNames(): array
    {
        return ['pg_expr_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_expr_test VALUES (2, 'Bob', 80, 20, 'B', '100%_done')");
        $this->pdo->exec("INSERT INTO pg_expr_test VALUES (3, 'Charlie', 70, 30, 'A', 'test_data')");
        $this->pdo->exec("INSERT INTO pg_expr_test VALUES (4, 'Diana', 60, 40, 'B', 'normal text')");
        $this->pdo->exec("INSERT INTO pg_expr_test VALUES (5, 'Eve', 50, NULL, 'C', NULL)");
    }

    /**
     * ORDER BY CASE expression.
     */
    public function testOrderByCaseExpression(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM pg_expr_test
            ORDER BY CASE category
                WHEN 'C' THEN 1
                WHEN 'A' THEN 2
                WHEN 'B' THEN 3
            END, name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Eve', $rows[0]); // C=1
    }

    /**
     * ORDER BY COALESCE expression.
     */
    public function testOrderByCoalesceExpression(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pg_expr_test ORDER BY COALESCE(bonus, 0)');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Eve', $rows[0]); // NULL → 0
    }

    /**
     * GROUP BY CASE expression.
     */
    public function testGroupByCaseExpression(): void
    {
        $stmt = $this->pdo->query("
            SELECT
                CASE WHEN score >= 80 THEN 'high' ELSE 'low' END AS tier,
                COUNT(*) AS cnt
            FROM pg_expr_test
            GROUP BY CASE WHEN score >= 80 THEN 'high' ELSE 'low' END
            ORDER BY tier
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('high', $rows[0]['tier']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    /**
     * HAVING with multiple aggregate conditions.
     */
    public function testHavingWithMultipleConditions(): void
    {
        $stmt = $this->pdo->query("
            SELECT category, COUNT(*) AS cnt, AVG(score) AS avg_score
            FROM pg_expr_test
            GROUP BY category
            HAVING COUNT(*) >= 2 AND AVG(score) >= 70
            ORDER BY category
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    /**
     * LIKE with ESCAPE clause for literal %.
     */
    public function testLikeWithEscapeForPercent(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_expr_test WHERE search_term LIKE '%!%%' ESCAPE '!'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $rows);
    }

    /**
     * ILIKE (PostgreSQL-specific case-insensitive LIKE).
     */
    public function testIlikeWithEscape(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_expr_test WHERE search_term ILIKE '%WORLD'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_expr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
