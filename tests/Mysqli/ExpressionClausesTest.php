<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests expression-based clauses in shadow queries on MySQLi:
 * - ORDER BY with expressions
 * - GROUP BY with CASE expression
 * - HAVING with multiple conditions
 * - LIKE with ESCAPE clause
 */
class ExpressionClausesTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_expr_test');
        $raw->query('CREATE TABLE mi_expr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, bonus INT, category VARCHAR(10), search_term VARCHAR(100))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (1, 'Alice', 90, 10, 'A', 'hello%world')");
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (2, 'Bob', 80, 20, 'B', '100%_done')");
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (3, 'Charlie', 70, 30, 'A', 'test_data')");
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (4, 'Diana', 60, 40, 'B', 'normal text')");
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (5, 'Eve', 50, NULL, 'C', NULL)");
    }

    /**
     * ORDER BY CASE expression.
     */
    public function testOrderByCaseExpression(): void
    {
        $result = $this->mysqli->query("
            SELECT name FROM mi_expr_test
            ORDER BY CASE category
                WHEN 'C' THEN 1
                WHEN 'A' THEN 2
                WHEN 'B' THEN 3
            END, name
        ");
        $row = $result->fetch_assoc();
        $this->assertSame('Eve', $row['name']); // C=1
    }

    /**
     * GROUP BY CASE expression.
     */
    public function testGroupByCaseExpression(): void
    {
        $result = $this->mysqli->query("
            SELECT
                CASE WHEN score >= 80 THEN 'high' ELSE 'low' END AS tier,
                COUNT(*) AS cnt
            FROM mi_expr_test
            GROUP BY CASE WHEN score >= 80 THEN 'high' ELSE 'low' END
            ORDER BY tier
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('high', $rows[0]['tier']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    /**
     * HAVING with multiple conditions.
     */
    public function testHavingWithMultipleConditions(): void
    {
        $result = $this->mysqli->query("
            SELECT category, COUNT(*) AS cnt
            FROM mi_expr_test
            GROUP BY category
            HAVING COUNT(*) >= 2 AND AVG(score) >= 70
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
    }

    /**
     * LIKE with ESCAPE for literal %.
     */
    public function testLikeWithEscapeForPercent(): void
    {
        $result = $this->mysqli->query("SELECT name FROM mi_expr_test WHERE search_term LIKE '%!%%' ESCAPE '!'");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_expr_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_expr_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
