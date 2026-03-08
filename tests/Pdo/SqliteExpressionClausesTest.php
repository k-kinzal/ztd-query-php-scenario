<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests expression-based clauses in shadow queries on SQLite:
 * - ORDER BY with expressions (arithmetic, CASE, COALESCE, functions)
 * - GROUP BY with expressions (CASE, arithmetic)
 * - HAVING with multiple aggregate conditions
 * - LIKE with ESCAPE clause
 *
 * These patterns are common in real-world reporting queries but were
 * previously untested with ZTD shadow store.
 */
class SqliteExpressionClausesTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE expr_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, bonus INTEGER, category TEXT, search_term TEXT)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO expr_test VALUES (1, 'Alice', 90, 10, 'A', 'hello%world')");
        $this->pdo->exec("INSERT INTO expr_test VALUES (2, 'Bob', 80, 20, 'B', '100%_done')");
        $this->pdo->exec("INSERT INTO expr_test VALUES (3, 'Charlie', 70, 30, 'A', 'test_data')");
        $this->pdo->exec("INSERT INTO expr_test VALUES (4, 'Diana', 60, 40, 'B', 'normal text')");
        $this->pdo->exec("INSERT INTO expr_test VALUES (5, 'Eve', 50, NULL, 'C', NULL)");
    }

    // --- ORDER BY with expressions ---

    /**
     * ORDER BY arithmetic expression.
     */
    public function testOrderByArithmeticExpression(): void
    {
        $stmt = $this->pdo->query('SELECT name, score + COALESCE(bonus, 0) AS total FROM expr_test ORDER BY score + COALESCE(bonus, 0) DESC');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Alice=100, Bob=100, Charlie=100, Diana=100 (all tied), Eve=50
        // Last row should be Eve with lowest total
        $this->assertSame('Eve', $rows[4]['name']);
        $this->assertSame(50, (int) $rows[4]['total']);
        // All others should be 100
        $this->assertSame(100, (int) $rows[0]['total']);
    }

    /**
     * ORDER BY CASE expression.
     */
    public function testOrderByCaseExpression(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM expr_test
            ORDER BY CASE category
                WHEN 'C' THEN 1
                WHEN 'A' THEN 2
                WHEN 'B' THEN 3
            END, name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Eve', $rows[0]); // C=1
        $this->assertSame('Alice', $rows[1]); // A=2
        $this->assertSame('Charlie', $rows[2]); // A=2
    }

    /**
     * ORDER BY COALESCE expression.
     */
    public function testOrderByCoalesceExpression(): void
    {
        $stmt = $this->pdo->query('SELECT name, bonus FROM expr_test ORDER BY COALESCE(bonus, 0)');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Eve', $rows[0]['name']); // NULL → 0
        $this->assertSame('Alice', $rows[1]['name']); // 10
    }

    /**
     * ORDER BY LENGTH() function.
     */
    public function testOrderByLengthFunction(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM expr_test ORDER BY LENGTH(name)');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Bob', $rows[0]); // 3 chars
        $this->assertSame('Eve', $rows[1]); // 3 chars
    }

    // --- GROUP BY with expressions ---

    /**
     * GROUP BY CASE expression.
     */
    public function testGroupByCaseExpression(): void
    {
        $stmt = $this->pdo->query("
            SELECT
                CASE WHEN score >= 80 THEN 'high' ELSE 'low' END AS tier,
                COUNT(*) AS cnt
            FROM expr_test
            GROUP BY CASE WHEN score >= 80 THEN 'high' ELSE 'low' END
            ORDER BY tier
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('high', $rows[0]['tier']);
        $this->assertSame(2, (int) $rows[0]['cnt']); // Alice=90, Bob=80
        $this->assertSame('low', $rows[1]['tier']);
        $this->assertSame(3, (int) $rows[1]['cnt']); // Charlie=70, Diana=60, Eve=50
    }

    /**
     * GROUP BY arithmetic expression.
     */
    public function testGroupByArithmeticExpression(): void
    {
        $stmt = $this->pdo->query('
            SELECT score / 10 AS decade, COUNT(*) AS cnt
            FROM expr_test
            GROUP BY score / 10
            ORDER BY decade DESC
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // 90/10=9, 80/10=8, 70/10=7, 60/10=6, 50/10=5
        $this->assertCount(5, $rows);
        $this->assertSame(9, (int) $rows[0]['decade']);
    }

    // --- HAVING with multiple conditions ---

    /**
     * HAVING with multiple aggregate conditions (AND).
     */
    public function testHavingWithMultipleConditions(): void
    {
        $stmt = $this->pdo->query("
            SELECT category, COUNT(*) AS cnt, AVG(score) AS avg_score
            FROM expr_test
            GROUP BY category
            HAVING COUNT(*) >= 2 AND AVG(score) >= 70
            ORDER BY category
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // A: 2 rows, avg=(90+70)/2=80 ✓
        // B: 2 rows, avg=(80+60)/2=70 ✓
        // C: 1 row → excluded by COUNT >= 2
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['category']);
        $this->assertSame('B', $rows[1]['category']);
    }

    /**
     * HAVING with SUM and COUNT combined.
     */
    public function testHavingWithSumAndCount(): void
    {
        $stmt = $this->pdo->query("
            SELECT category, SUM(score) AS total, COUNT(*) AS cnt
            FROM expr_test
            GROUP BY category
            HAVING SUM(score) > 100 AND COUNT(*) > 1
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // A: sum=160, cnt=2 ✓
        // B: sum=140, cnt=2 ✓
        // C: sum=50, cnt=1 → excluded
        $this->assertCount(2, $rows);
    }

    // --- LIKE with ESCAPE ---

    /**
     * LIKE with ESCAPE clause for literal % search.
     */
    public function testLikeWithEscapeForPercent(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM expr_test WHERE search_term LIKE '%!%%' ESCAPE '!'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Should match 'hello%world' and '100%_done' (both contain literal %)
        $this->assertCount(2, $rows);
    }

    /**
     * LIKE with ESCAPE clause for literal _ search.
     */
    public function testLikeWithEscapeForUnderscore(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM expr_test WHERE search_term LIKE '%!_%' ESCAPE '!'");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Should match '100%_done' and 'test_data' (both contain literal _)
        $this->assertCount(2, $rows);
    }

    /**
     * LIKE without ESCAPE (standard behavior).
     */
    public function testLikeWithoutEscape(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM expr_test WHERE search_term LIKE '%world'");
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
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM expr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
