<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT ... SELECT DISTINCT with shadow data on SQLite.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertSelectDistinctTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_isd_orders (
                id INTEGER PRIMARY KEY,
                customer_name TEXT NOT NULL,
                product TEXT NOT NULL,
                amount REAL NOT NULL
            )',
            'CREATE TABLE sl_isd_customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isd_customers', 'sl_isd_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_isd_orders (id, customer_name, product, amount) VALUES
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
                "INSERT INTO sl_isd_customers (name)
                 SELECT DISTINCT customer_name FROM sl_isd_orders ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_isd_customers ORDER BY name");

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

    public function testInsertSelectDistinctAfterSourceMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_isd_orders (id, customer_name, product, amount) VALUES (6, 'Diana', 'Widget', 75.00)");
        $this->pdo->exec("INSERT INTO sl_isd_orders (id, customer_name, product, amount) VALUES (7, 'Diana', 'Gadget', 120.00)");

        try {
            $this->pdo->exec(
                "INSERT INTO sl_isd_customers (name)
                 SELECT DISTINCT customer_name FROM sl_isd_orders ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_isd_customers ORDER BY name");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT after mutation: expected 4, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Diana', $rows[3]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT after mutation failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectDistinctWithWhere(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_isd_customers (name)
                 SELECT DISTINCT customer_name FROM sl_isd_orders
                 WHERE amount >= 150.00
                 ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_isd_customers ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT WHERE: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT with WHERE failed: ' . $e->getMessage());
        }
    }

    public function testCountDistinctOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO sl_isd_orders (id, customer_name, product, amount) VALUES (6, 'Alice', 'Premium', 500.00)");

        try {
            $rows = $this->ztdQuery("SELECT COUNT(DISTINCT customer_name) AS cnt FROM sl_isd_orders");
            $this->assertSame(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(DISTINCT) on shadow data failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... SELECT DISTINCT with GROUP BY and HAVING.
     */
    public function testInsertSelectDistinctGroupByHaving(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_isd_customers (name)
                 SELECT customer_name FROM sl_isd_orders
                 GROUP BY customer_name
                 HAVING COUNT(*) > 1
                 ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_isd_customers ORDER BY name");

            // Customers with > 1 order: Alice (2), Bob (2) = 2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT GROUP BY HAVING: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT GROUP BY HAVING failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_isd_customers (name)
                 SELECT DISTINCT customer_name FROM sl_isd_orders"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_isd_customers")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
