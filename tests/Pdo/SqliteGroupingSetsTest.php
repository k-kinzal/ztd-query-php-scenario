<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that GROUPING SETS / ROLLUP / CUBE are NOT supported on SQLite.
 *
 * SQLite does not support these advanced GROUP BY extensions.
 * These queries should throw errors.
 * @spec SPEC-3.1
 */
class SqliteGroupingSetsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE gs_test (id INT PRIMARY KEY, region VARCHAR(20), product VARCHAR(20), amount INT)';
    }

    protected function getTableNames(): array
    {
        return ['gs_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO gs_test VALUES (1, 'East', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO gs_test VALUES (2, 'East', 'Gadget', 200)");
        $this->pdo->exec("INSERT INTO gs_test VALUES (3, 'West', 'Widget', 150)");
    }
    /**
     * GROUPING SETS not supported on SQLite — should throw.
     */
    public function testGroupingSetsThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query(
            'SELECT region, SUM(amount) FROM gs_test GROUP BY GROUPING SETS ((region), ())'
        );
    }

    /**
     * ROLLUP not supported on SQLite — should throw.
     */
    public function testRollupThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query(
            'SELECT region, product, SUM(amount) FROM gs_test GROUP BY ROLLUP (region, product)'
        );
    }

    /**
     * CUBE not supported on SQLite — should throw.
     */
    public function testCubeThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query(
            'SELECT region, product, SUM(amount) FROM gs_test GROUP BY CUBE (region, product)'
        );
    }

    /**
     * Regular GROUP BY still works fine.
     */
    public function testRegularGroupByWorks(): void
    {
        $stmt = $this->pdo->query('SELECT region, SUM(amount) as total FROM gs_test GROUP BY region ORDER BY region');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('East', $rows[0]['region']);
        $this->assertEquals(300, (int) $rows[0]['total']);
    }
}
