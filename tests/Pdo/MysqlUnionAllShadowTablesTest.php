<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UNION ALL between multiple shadow tables on MySQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class MysqlUnionAllShadowTablesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_uas_orders (
                id INT PRIMARY KEY,
                customer VARCHAR(100) NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_uas_refunds (
                id INT PRIMARY KEY,
                customer VARCHAR(100) NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_uas_refunds', 'my_uas_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_uas_orders VALUES (1, 'Alice', 100.00)");
        $this->ztdExec("INSERT INTO my_uas_orders VALUES (2, 'Bob', 200.00)");
        $this->ztdExec("INSERT INTO my_uas_refunds VALUES (1, 'Alice', 30.00)");
        $this->ztdExec("INSERT INTO my_uas_refunds VALUES (2, 'Bob', 50.00)");
    }

    /**
     * UNION ALL between two shadow tables.
     */
    public function testUnionAllBetweenShadowTables(): void
    {
        $rows = $this->ztdQuery(
            "SELECT 'order' AS type, customer, amount FROM my_uas_orders
             UNION ALL
             SELECT 'refund' AS type, customer, amount FROM my_uas_refunds
             ORDER BY customer, type"
        );

        $this->assertCount(4, $rows);
    }

    /**
     * UNION ALL after shadow mutation.
     */
    public function testUnionAllAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO my_uas_refunds VALUES (3, 'Carol', 75.00)");

        $rows = $this->ztdQuery(
            "SELECT customer, amount FROM my_uas_refunds
             UNION ALL
             SELECT customer, amount FROM my_uas_orders
             ORDER BY customer"
        );

        $this->assertCount(5, $rows);
    }

    /**
     * UNION with WHERE on both branches.
     */
    public function testUnionAllWithWhereOnBranches(): void
    {
        $rows = $this->ztdQuery(
            "SELECT customer, amount FROM my_uas_orders WHERE amount > 100
             UNION ALL
             SELECT customer, amount FROM my_uas_refunds WHERE amount > 30
             ORDER BY customer"
        );

        // Orders > 100: Bob(200) = 1
        // Refunds > 30: Bob(50) = 1
        $this->assertCount(2, $rows);
    }

    /**
     * UNION DISTINCT between shadow tables.
     */
    public function testUnionDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT customer FROM my_uas_orders
             UNION
             SELECT customer FROM my_uas_refunds
             ORDER BY customer"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertSame('Bob', $rows[1]['customer']);
    }

    /**
     * UNION subquery in WHERE.
     */
    public function testUnionSubqueryInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT * FROM my_uas_orders
             WHERE customer IN (
                SELECT customer FROM my_uas_refunds
             )
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM my_uas_orders')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
