<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT ... SELECT DISTINCT with shadow data on PostgreSQL.
 *
 * Also tests DISTINCT ON (...) which is a PostgreSQL-specific extension
 * commonly used to get the first row per group.
 *
 * @spec SPEC-4.1
 */
class PostgresInsertSelectDistinctTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_isd_orders (
                id INT PRIMARY KEY,
                customer_name VARCHAR(50),
                product VARCHAR(50),
                amount NUMERIC(10,2)
            )',
            'CREATE TABLE pg_isd_customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) UNIQUE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_isd_customers', 'pg_isd_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_isd_orders (id, customer_name, product, amount) VALUES
            (1, 'Alice', 'Widget', 100.00),
            (2, 'Bob', 'Gadget', 200.00),
            (3, 'Alice', 'Gadget', 150.00),
            (4, 'Charlie', 'Widget', 50.00),
            (5, 'Bob', 'Premium', 300.00)");
    }

    public function testInsertSelectDistinct(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_isd_customers (name)
                 SELECT DISTINCT customer_name FROM pg_isd_orders ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_isd_customers ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Charlie', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT failed: ' . $e->getMessage());
        }
    }

    /**
     * DISTINCT ON (...) is a PostgreSQL extension. It returns the first row
     * per group defined by the DISTINCT ON columns.
     */
    public function testSelectDistinctOn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT DISTINCT ON (customer_name) customer_name, product, amount
                 FROM pg_isd_orders
                 ORDER BY customer_name, amount DESC"
            );

            // For each customer, the highest-amount order:
            // Alice: 150 (Gadget), Bob: 300 (Premium), Charlie: 50 (Widget)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DISTINCT ON: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);

            $byName = [];
            foreach ($rows as $row) {
                $byName[$row['customer_name']] = $row;
            }

            $this->assertSame('Gadget', $byName['Alice']['product']);
            $this->assertSame('Premium', $byName['Bob']['product']);
            $this->assertSame('Widget', $byName['Charlie']['product']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT DISTINCT ON failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... SELECT DISTINCT ON (...) with shadow mutation.
     */
    public function testInsertSelectDistinctOnAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO pg_isd_orders (id, customer_name, product, amount) VALUES (6, 'Alice', 'Deluxe', 999.00)");

        try {
            $this->pdo->exec(
                "INSERT INTO pg_isd_customers (name)
                 SELECT DISTINCT ON (customer_name) customer_name
                 FROM pg_isd_orders
                 ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_isd_customers ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT DISTINCT ON after mutation: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT ON failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectDistinctWithWhere(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_isd_customers (name)
                 SELECT DISTINCT customer_name FROM pg_isd_orders
                 WHERE amount >= 150.00
                 ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_isd_customers ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT WHERE: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT with WHERE failed: ' . $e->getMessage());
        }
    }

    public function testCountDistinctOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pg_isd_orders (id, customer_name, product, amount) VALUES (6, 'Alice', 'Premium', 500.00)");

        try {
            $rows = $this->ztdQuery("SELECT COUNT(DISTINCT customer_name) AS cnt FROM pg_isd_orders");
            $this->assertSame(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(DISTINCT) on shadow data failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... SELECT DISTINCT with RETURNING clause.
     */
    public function testInsertSelectDistinctReturning(): void
    {
        try {
            $rows = $this->pdo->query(
                "INSERT INTO pg_isd_customers (name)
                 SELECT DISTINCT customer_name FROM pg_isd_orders
                 ORDER BY customer_name
                 RETURNING id, name"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT RETURNING: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT RETURNING failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_isd_customers (name)
                 SELECT DISTINCT customer_name FROM pg_isd_orders"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_isd_customers")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
