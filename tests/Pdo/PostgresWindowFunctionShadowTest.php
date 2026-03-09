<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests window functions on shadow data (PostgreSQL).
 *
 * PostgreSQL has rich window function support. This verifies that
 * window functions work correctly with CTE-rewritten shadow data.
 *
 * @spec SPEC-3.3
 */
class PostgresWindowFunctionShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE wf_pg (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['wf_pg'];
    }

    private function seedData(): void
    {
        $this->pdo->exec("INSERT INTO wf_pg (id, name, dept, salary) VALUES
            (1, 'Alice', 'Engineering', 90000),
            (2, 'Bob', 'Engineering', 85000),
            (3, 'Charlie', 'Sales', 70000),
            (4, 'Diana', 'Sales', 75000),
            (5, 'Eve', 'Engineering', 95000)");
    }

    /**
     * ROW_NUMBER() on shadow data.
     */
    public function testRowNumber(): void
    {
        $this->seedData();

        $rows = $this->ztdQuery(
            'SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM wf_pg'
        );
        $this->assertCount(5, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['rn']);
    }

    /**
     * DENSE_RANK() on shadow data.
     */
    public function testDenseRank(): void
    {
        $this->pdo->exec("INSERT INTO wf_pg (id, name, dept, salary) VALUES
            (1, 'A', 'X', 100), (2, 'B', 'X', 100), (3, 'C', 'X', 90)");

        $rows = $this->ztdQuery(
            'SELECT name, DENSE_RANK() OVER (ORDER BY salary DESC) AS dr FROM wf_pg'
        );
        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['dr']);
        $this->assertEquals(1, (int) $rows[1]['dr']);
        $this->assertEquals(2, (int) $rows[2]['dr']); // DENSE_RANK gives 2, not 3
    }

    /**
     * NTILE() on shadow data.
     */
    public function testNtile(): void
    {
        $this->seedData();

        $rows = $this->ztdQuery(
            'SELECT name, NTILE(2) OVER (ORDER BY salary) AS tile FROM wf_pg ORDER BY salary'
        );
        $this->assertCount(5, $rows);
        // First 3 in tile 1, last 2 in tile 2
        $this->assertEquals(1, (int) $rows[0]['tile']);
        $this->assertEquals(2, (int) $rows[4]['tile']);
    }

    /**
     * Window function with PARTITION BY after mutation.
     */
    public function testWindowPartitionAfterMutation(): void
    {
        $this->seedData();
        // Move Charlie to Engineering
        $this->pdo->exec("UPDATE wf_pg SET dept = 'Engineering' WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
             FROM wf_pg WHERE dept = 'Engineering' ORDER BY rn"
        );
        $this->assertCount(4, $rows); // 3 original + Charlie moved in
        $this->assertEquals(1, (int) $rows[0]['rn']);
    }

    /**
     * AVG() OVER with ROWS BETWEEN on shadow data.
     */
    public function testAvgOverRowsBetween(): void
    {
        $this->pdo->exec("INSERT INTO wf_pg (id, name, dept, salary) VALUES
            (1, 'A', 'X', 10), (2, 'B', 'X', 20), (3, 'C', 'X', 30), (4, 'D', 'X', 40), (5, 'E', 'X', 50)");

        $rows = $this->ztdQuery(
            'SELECT name, AVG(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg FROM wf_pg'
        );
        $this->assertCount(5, $rows);
        // Row 1: AVG(10,20) = 15
        // Row 2: AVG(10,20,30) = 20
        // Row 3: AVG(20,30,40) = 30
        $this->assertEqualsWithDelta(15.0, (float) $rows[0]['moving_avg'], 0.5);
        $this->assertEqualsWithDelta(20.0, (float) $rows[1]['moving_avg'], 0.5);
        $this->assertEqualsWithDelta(30.0, (float) $rows[2]['moving_avg'], 0.5);
    }
}
