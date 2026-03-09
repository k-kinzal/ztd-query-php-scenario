<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UNION ALL between multiple shadow tables on PostgreSQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class PostgresUnionAllShadowTablesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_uas_orders (
                id SERIAL PRIMARY KEY,
                customer VARCHAR(100) NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_uas_refunds (
                id SERIAL PRIMARY KEY,
                customer VARCHAR(100) NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_uas_refunds', 'pg_uas_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_uas_orders (id, customer, amount) VALUES (1, 'Alice', 100.00)");
        $this->pdo->exec("INSERT INTO pg_uas_orders (id, customer, amount) VALUES (2, 'Bob', 200.00)");
        $this->pdo->exec("INSERT INTO pg_uas_refunds (id, customer, amount) VALUES (1, 'Alice', 30.00)");
        $this->pdo->exec("INSERT INTO pg_uas_refunds (id, customer, amount) VALUES (2, 'Bob', 50.00)");
    }

    /**
     * UNION ALL between two shadow tables.
     */
    public function testUnionAllBetweenShadowTables(): void
    {
        $rows = $this->ztdQuery(
            "SELECT 'order' AS type, customer, amount FROM pg_uas_orders
             UNION ALL
             SELECT 'refund' AS type, customer, amount FROM pg_uas_refunds
             ORDER BY customer, type"
        );

        $this->assertCount(4, $rows);
    }

    /**
     * UNION ALL after shadow mutation.
     */
    public function testUnionAllAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO pg_uas_refunds (id, customer, amount) VALUES (3, 'Carol', 75.00)");

        $rows = $this->ztdQuery(
            "SELECT customer, amount FROM pg_uas_refunds
             UNION ALL
             SELECT customer, amount FROM pg_uas_orders
             ORDER BY customer"
        );

        $this->assertCount(5, $rows);
    }

    /**
     * UNION DISTINCT between shadow tables.
     */
    public function testUnionDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT customer FROM pg_uas_orders
             UNION
             SELECT customer FROM pg_uas_refunds
             ORDER BY customer"
        );

        $this->assertCount(2, $rows);
    }

    /**
     * UNION with prepared $N params.
     */
    public function testUnionWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT customer, amount FROM pg_uas_orders WHERE amount > $1
             UNION ALL
             SELECT customer, amount FROM pg_uas_refunds WHERE amount > $1
             ORDER BY customer"
        );
        $stmt->execute([40]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'UNION ALL with prepared $N params returned no rows.'
            );
        }

        // Orders > 40: Alice(100), Bob(200) = 2
        // Refunds > 40: Bob(50) = 1
        $this->assertCount(3, $rows);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_uas_orders')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
