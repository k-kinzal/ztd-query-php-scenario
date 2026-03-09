<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE patterns where multiple SET columns reference each other's
 * original values through the SQLite CTE shadow store.
 *
 * @spec SPEC-4.2
 */
class SqliteColumnSwapUpdateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_swap (
            id INTEGER PRIMARY KEY,
            col_a INTEGER NOT NULL,
            col_b INTEGER NOT NULL,
            label TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_swap'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_swap VALUES (1, 10, 20, 'first')");
        $this->pdo->exec("INSERT INTO sl_swap VALUES (2, 30, 40, 'second')");
        $this->pdo->exec("INSERT INTO sl_swap VALUES (3, 50, 60, 'third')");
    }

    /**
     * Swap two column values.
     */
    public function testSwapTwoColumns(): void
    {
        $this->pdo->exec(
            "UPDATE sl_swap SET col_a = col_b, col_b = col_a WHERE id = 1"
        );
        $rows = $this->ztdQuery("SELECT col_a, col_b FROM sl_swap WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(20, (int) $rows[0]['col_a']);
        $this->assertEquals(10, (int) $rows[0]['col_b']);
    }

    /**
     * Swap all rows.
     */
    public function testSwapAllRows(): void
    {
        $this->pdo->exec("UPDATE sl_swap SET col_a = col_b, col_b = col_a");
        $rows = $this->ztdQuery("SELECT id, col_a, col_b FROM sl_swap ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertEquals(20, (int) $rows[0]['col_a']);
        $this->assertEquals(10, (int) $rows[0]['col_b']);
        $this->assertEquals(40, (int) $rows[1]['col_a']);
        $this->assertEquals(30, (int) $rows[1]['col_b']);
    }

    /**
     * Cross-referencing arithmetic.
     */
    public function testCrossReferencingArithmetic(): void
    {
        $this->pdo->exec(
            "UPDATE sl_swap SET col_a = col_a + col_b, col_b = col_a - col_b WHERE id = 1"
        );
        $rows = $this->ztdQuery("SELECT col_a, col_b FROM sl_swap WHERE id = 1");
        $this->assertEquals(30, (int) $rows[0]['col_a']);
        $this->assertEquals(-10, (int) $rows[0]['col_b']);
    }

    /**
     * SET using CASE.
     */
    public function testConditionalSwap(): void
    {
        $this->pdo->exec(
            "UPDATE sl_swap SET
                col_a = CASE WHEN col_a > col_b THEN col_b ELSE col_a END,
                col_b = CASE WHEN col_a > col_b THEN col_a ELSE col_b END"
        );
        $rows = $this->ztdQuery("SELECT id, col_a, col_b FROM sl_swap ORDER BY id");
        $this->assertEquals(10, (int) $rows[0]['col_a']);
        $this->assertEquals(20, (int) $rows[0]['col_b']);
    }

    /**
     * SET using || string concatenation.
     */
    public function testSetWithStringConcat(): void
    {
        $this->pdo->exec(
            "UPDATE sl_swap SET label = label || '-v2', col_a = col_b * 2 WHERE id = 2"
        );
        $rows = $this->ztdQuery("SELECT col_a, label FROM sl_swap WHERE id = 2");
        $this->assertEquals(80, (int) $rows[0]['col_a']);
        $this->assertSame('second-v2', $rows[0]['label']);
    }

    /**
     * Double swap restores original.
     */
    public function testDoubleSwapRestoresOriginal(): void
    {
        $this->pdo->exec("UPDATE sl_swap SET col_a = col_b, col_b = col_a WHERE id = 1");
        $this->pdo->exec("UPDATE sl_swap SET col_a = col_b, col_b = col_a WHERE id = 1");

        $rows = $this->ztdQuery("SELECT col_a, col_b FROM sl_swap WHERE id = 1");
        $this->assertEquals(10, (int) $rows[0]['col_a']);
        $this->assertEquals(20, (int) $rows[0]['col_b']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_swap');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
