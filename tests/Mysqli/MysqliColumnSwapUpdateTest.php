<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE column swap patterns through MySQLi CTE shadow store.
 *
 * @spec SPEC-4.2
 */
class MysqliColumnSwapUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_swap (
            id INT PRIMARY KEY,
            col_a INT NOT NULL,
            col_b INT NOT NULL,
            label VARCHAR(30)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_swap'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_swap VALUES (1, 10, 20, 'first')");
        $this->mysqli->query("INSERT INTO mi_swap VALUES (2, 30, 40, 'second')");
        $this->mysqli->query("INSERT INTO mi_swap VALUES (3, 50, 60, 'third')");
    }

    /**
     * Swap two column values.
     */
    public function testSwapTwoColumns(): void
    {
        $this->mysqli->query("UPDATE mi_swap SET col_a = col_b, col_b = col_a WHERE id = 1");
        $rows = $this->ztdQuery("SELECT col_a, col_b FROM mi_swap WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEquals(20, (int) $rows[0]['col_a']);
        $this->assertEquals(10, (int) $rows[0]['col_b']);
    }

    /**
     * Cross-referencing arithmetic.
     */
    public function testCrossReferencingArithmetic(): void
    {
        $this->mysqli->query(
            "UPDATE mi_swap SET col_a = col_a + col_b, col_b = col_a - col_b WHERE id = 1"
        );
        $rows = $this->ztdQuery("SELECT col_a, col_b FROM mi_swap WHERE id = 1");
        $this->assertEquals(30, (int) $rows[0]['col_a']);
        $this->assertEquals(-10, (int) $rows[0]['col_b']);
    }

    /**
     * Double swap restores original.
     */
    public function testDoubleSwapRestoresOriginal(): void
    {
        $this->mysqli->query("UPDATE mi_swap SET col_a = col_b, col_b = col_a WHERE id = 1");
        $this->mysqli->query("UPDATE mi_swap SET col_a = col_b, col_b = col_a WHERE id = 1");
        $rows = $this->ztdQuery("SELECT col_a, col_b FROM mi_swap WHERE id = 1");
        $this->assertEquals(10, (int) $rows[0]['col_a']);
        $this->assertEquals(20, (int) $rows[0]['col_b']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_swap');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
