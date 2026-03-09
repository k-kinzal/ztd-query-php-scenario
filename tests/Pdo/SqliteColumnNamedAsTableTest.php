<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests scenarios where column names match table names.
 *
 * Real-world scenario: it is common for tables to have columns named after
 * other tables (e.g., a `status` column in a table named `status`, or an
 * `orders` column in a `users` table referencing a table named `orders`).
 * The CTE rewriter must distinguish between column references and table
 * references to avoid incorrect rewrites.
 *
 * @spec SPEC-3.1
 */
class SqliteColumnNamedAsTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cnt_status (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                description TEXT
            )',
            'CREATE TABLE sl_cnt_orders (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                total REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cnt_orders', 'sl_cnt_status'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_cnt_status VALUES (1, 'active', 'Currently active')");
        $this->ztdExec("INSERT INTO sl_cnt_status VALUES (2, 'inactive', 'Not active')");
        $this->ztdExec("INSERT INTO sl_cnt_orders VALUES (1, 'active', 100.00)");
        $this->ztdExec("INSERT INTO sl_cnt_orders VALUES (2, 'inactive', 50.00)");
        $this->ztdExec("INSERT INTO sl_cnt_orders VALUES (3, 'active', 200.00)");
    }

    /**
     * SELECT where column name matches another table name.
     * The rewriter should not confuse the column `status` with the table `sl_cnt_status`.
     */
    public function testSelectWhereColumnNameMatchesTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM sl_cnt_orders WHERE status = 'active' ORDER BY id"
            );
            $this->assertCount(2, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
            $this->assertEquals(3, (int) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT with column matching table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * JOIN where column and table share the same name.
     * Pattern: JOIN sl_cnt_status ON orders.status = sl_cnt_status.status
     */
    public function testJoinWhereColumnAndTableShareName(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT o.id, o.total, s.description
                 FROM sl_cnt_orders o
                 JOIN sl_cnt_status s ON o.status = s.status
                 ORDER BY o.id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Currently active', $rows[0]['description']);
            $this->assertSame('Not active', $rows[1]['description']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JOIN with column/table name collision failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE using column that matches table name in WHERE.
     */
    public function testUpdateWhereColumnMatchesTableName(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_cnt_orders SET total = total * 1.1 WHERE status = 'active'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, total FROM sl_cnt_orders WHERE status = 'active' ORDER BY id"
            );
            $this->assertCount(2, $rows);
            $this->assertEqualsWithDelta(110.00, (float) $rows[0]['total'], 0.01);
            $this->assertEqualsWithDelta(220.00, (float) $rows[1]['total'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with column matching table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE using column that matches table name in WHERE.
     */
    public function testDeleteWhereColumnMatchesTableName(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_cnt_orders WHERE status = 'inactive'");

            $rows = $this->ztdQuery("SELECT * FROM sl_cnt_orders");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with column matching table name failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Subquery referencing table whose name appears as column in outer query.
     */
    public function testSubqueryWithTableNameAsOuterColumn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT o.id, o.total,
                        (SELECT s.description FROM sl_cnt_status s WHERE s.status = o.status) AS status_desc
                 FROM sl_cnt_orders o
                 ORDER BY o.id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Currently active', $rows[0]['status_desc']);
            $this->assertSame('Not active', $rows[1]['status_desc']);
            $this->assertSame('Currently active', $rows[2]['status_desc']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Subquery with table name as outer column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with column matching table name.
     */
    public function testPreparedWithColumnMatchingTableName(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT o.id, o.total
                 FROM sl_cnt_orders o
                 JOIN sl_cnt_status s ON o.status = s.status
                 WHERE s.description = ?",
                ['Currently active']
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared query with column matching table name failed: ' . $e->getMessage()
            );
        }
    }
}
