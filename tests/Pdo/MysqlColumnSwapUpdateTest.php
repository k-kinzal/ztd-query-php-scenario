<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE patterns where multiple SET columns reference each other's
 * original values, including value swaps and self-referencing arithmetic.
 *
 * In standard SQL, all SET expressions reference the row's pre-update values.
 * The CTE rewriter must preserve this semantics.
 *
 * @spec SPEC-4.2
 */
class MysqlColumnSwapUpdateTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_swap (
            id INT PRIMARY KEY,
            col_a INT NOT NULL,
            col_b INT NOT NULL,
            label VARCHAR(30)
        )';
    }

    protected function getTableNames(): array
    {
        return ['my_swap'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_swap VALUES (1, 10, 20, 'first')");
        $this->pdo->exec("INSERT INTO my_swap VALUES (2, 30, 40, 'second')");
        $this->pdo->exec("INSERT INTO my_swap VALUES (3, 50, 60, 'third')");
    }

    /**
     * Swap two column values: SET a=b, b=a.
     * Standard SQL requires both SET expressions to use original values.
     */
    public function testSwapTwoColumns(): void
    {
        $this->pdo->exec(
            "UPDATE my_swap SET col_a = col_b, col_b = col_a WHERE id = 1"
        );
        $rows = $this->ztdQuery("SELECT col_a, col_b FROM my_swap WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(20, (int) $rows[0]['col_a'], 'col_a should now be original col_b');
        $this->assertEquals(10, (int) $rows[0]['col_b'], 'col_b should now be original col_a');
    }

    /**
     * Swap all rows at once.
     */
    public function testSwapAllRows(): void
    {
        $this->pdo->exec("UPDATE my_swap SET col_a = col_b, col_b = col_a");
        $rows = $this->ztdQuery("SELECT id, col_a, col_b FROM my_swap ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertEquals(20, (int) $rows[0]['col_a']);
        $this->assertEquals(10, (int) $rows[0]['col_b']);
        $this->assertEquals(40, (int) $rows[1]['col_a']);
        $this->assertEquals(30, (int) $rows[1]['col_b']);
        $this->assertEquals(60, (int) $rows[2]['col_a']);
        $this->assertEquals(50, (int) $rows[2]['col_b']);
    }

    /**
     * Cross-referencing arithmetic: SET a = a + b, b = a - b.
     * After: a = orig_a + orig_b, b = orig_a - orig_b (NOT updated a - orig_b).
     */
    public function testCrossReferencingArithmetic(): void
    {
        $this->pdo->exec(
            "UPDATE my_swap SET col_a = col_a + col_b, col_b = col_a - col_b WHERE id = 1"
        );
        $rows = $this->ztdQuery("SELECT col_a, col_b FROM my_swap WHERE id = 1");
        $this->assertCount(1, $rows);
        // orig: a=10, b=20
        // Standard SQL: a = 10+20 = 30, b = 10-20 = -10
        $this->assertEquals(30, (int) $rows[0]['col_a']);
        $this->assertEquals(-10, (int) $rows[0]['col_b']);
    }

    /**
     * SET using CASE referencing both columns.
     */
    public function testConditionalSwap(): void
    {
        $this->pdo->exec(
            "UPDATE my_swap SET
                col_a = CASE WHEN col_a > col_b THEN col_b ELSE col_a END,
                col_b = CASE WHEN col_a > col_b THEN col_a ELSE col_b END"
        );
        // Row 1: a=10, b=20 -> a>b? No -> unchanged
        // Row 2: a=30, b=40 -> a>b? No -> unchanged
        // Row 3: a=50, b=60 -> a>b? No -> unchanged
        $rows = $this->ztdQuery("SELECT id, col_a, col_b FROM my_swap ORDER BY id");
        $this->assertEquals(10, (int) $rows[0]['col_a']);
        $this->assertEquals(20, (int) $rows[0]['col_b']);
        $this->assertEquals(50, (int) $rows[2]['col_a']);
        $this->assertEquals(60, (int) $rows[2]['col_b']);
    }

    /**
     * UPDATE SET from another column plus literal.
     */
    public function testSetFromOtherColumnPlusLiteral(): void
    {
        $this->pdo->exec(
            "UPDATE my_swap SET col_a = col_b * 2, label = CONCAT(label, '-updated') WHERE id = 2"
        );
        $rows = $this->ztdQuery("SELECT col_a, col_b, label FROM my_swap WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertEquals(80, (int) $rows[0]['col_a']);
        $this->assertEquals(40, (int) $rows[1 - 1]['col_b']); // unchanged
        $this->assertSame('second-updated', $rows[0]['label']);
    }

    /**
     * Double swap: swap, then swap back.
     */
    public function testDoubleSwapRestoresOriginal(): void
    {
        $this->pdo->exec("UPDATE my_swap SET col_a = col_b, col_b = col_a WHERE id = 1");
        $this->pdo->exec("UPDATE my_swap SET col_a = col_b, col_b = col_a WHERE id = 1");

        $rows = $this->ztdQuery("SELECT col_a, col_b FROM my_swap WHERE id = 1");
        $this->assertEquals(10, (int) $rows[0]['col_a'], 'Double swap should restore original');
        $this->assertEquals(20, (int) $rows[0]['col_b'], 'Double swap should restore original');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_swap');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
