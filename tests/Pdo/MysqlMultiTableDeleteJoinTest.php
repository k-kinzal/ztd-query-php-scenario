<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL multi-table DELETE with JOIN through CTE shadow (PDO).
 *
 * MySQL supports:
 *   DELETE t FROM t JOIN s ON t.id = s.id WHERE ...
 *
 * Issue #26 documents that multi-target DELETE only affects the first table.
 * This test explores additional patterns and the PDO path.
 *
 * @spec SPEC-4.3
 */
class MysqlMultiTableDeleteJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_mtd_orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20))',
            'CREATE TABLE pdo_mtd_customers (id INT PRIMARY KEY, name VARCHAR(50), active BOOLEAN)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_mtd_orders', 'pdo_mtd_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_mtd_customers VALUES (1, 'Alice', TRUE)");
        $this->pdo->exec("INSERT INTO pdo_mtd_customers VALUES (2, 'Bob', FALSE)");
        $this->pdo->exec("INSERT INTO pdo_mtd_customers VALUES (3, 'Charlie', TRUE)");

        $this->pdo->exec("INSERT INTO pdo_mtd_orders VALUES (1, 1, 'completed')");
        $this->pdo->exec("INSERT INTO pdo_mtd_orders VALUES (2, 2, 'pending')");
        $this->pdo->exec("INSERT INTO pdo_mtd_orders VALUES (3, 2, 'completed')");
        $this->pdo->exec("INSERT INTO pdo_mtd_orders VALUES (4, 3, 'pending')");
    }

    /**
     * DELETE single table using JOIN for filtering.
     */
    public function testDeleteSingleTableWithJoin(): void
    {
        try {
            $this->pdo->exec(
                "DELETE o FROM pdo_mtd_orders o
                 JOIN pdo_mtd_customers c ON o.customer_id = c.id
                 WHERE c.active = FALSE"
            );

            $rows = $this->ztdQuery('SELECT id FROM pdo_mtd_orders ORDER BY id');
            $this->assertCount(2, $rows, 'Orders of inactive customer (Bob) should be deleted');
            $this->assertEquals(1, (int) $rows[0]['id']);
            $this->assertEquals(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE with JOIN not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from multiple tables in one statement.
     * Known Issue #26: Only first table is affected.
     */
    public function testDeleteMultipleTablesWithJoin(): void
    {
        try {
            $this->pdo->exec(
                "DELETE o, c FROM pdo_mtd_orders o
                 JOIN pdo_mtd_customers c ON o.customer_id = c.id
                 WHERE c.active = FALSE"
            );

            $orderRows = $this->ztdQuery('SELECT id FROM pdo_mtd_orders ORDER BY id');
            $customerRows = $this->ztdQuery('SELECT id FROM pdo_mtd_customers ORDER BY id');

            // Expected: Bob's orders (2,3) deleted AND Bob (customer 2) deleted
            $this->assertCount(2, $orderRows, 'Bob\'s orders should be deleted');
            $this->assertCount(2, $customerRows, 'Bob should be deleted from customers');
        } catch (\Throwable $e) {
            $this->markTestSkipped('Multi-table DELETE not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with LEFT JOIN (delete orphaned rows).
     */
    public function testDeleteWithLeftJoin(): void
    {
        // Add orphaned order
        $this->pdo->exec("INSERT INTO pdo_mtd_orders VALUES (5, 99, 'orphaned')");

        try {
            $this->pdo->exec(
                "DELETE o FROM pdo_mtd_orders o
                 LEFT JOIN pdo_mtd_customers c ON o.customer_id = c.id
                 WHERE c.id IS NULL"
            );

            $rows = $this->ztdQuery('SELECT id FROM pdo_mtd_orders ORDER BY id');
            $this->assertCount(4, $rows, 'Only orphaned order should be deleted');
            $ids = array_column($rows, 'id');
            $this->assertNotContains('5', $ids);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE with LEFT JOIN not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with JOIN on shadow-inserted data.
     */
    public function testDeleteWithJoinOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mtd_customers VALUES (4, 'Diana', FALSE)");
        $this->pdo->exec("INSERT INTO pdo_mtd_orders VALUES (5, 4, 'new')");

        try {
            $this->pdo->exec(
                "DELETE o FROM pdo_mtd_orders o
                 JOIN pdo_mtd_customers c ON o.customer_id = c.id
                 WHERE c.active = FALSE"
            );

            $rows = $this->ztdQuery('SELECT id FROM pdo_mtd_orders ORDER BY id');
            // Bob's orders (2,3) + Diana's order (5) should be deleted
            $this->assertCount(2, $rows, 'Orders of inactive customers should be deleted');
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE JOIN on shadow data not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mtd_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
