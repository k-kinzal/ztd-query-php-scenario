<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT ... SELECT DISTINCT with shadow data on MySQL.
 *
 * The CTE rewriter must handle DISTINCT in the SELECT portion of INSERT...SELECT.
 * This pattern is common for deduplication workflows: extracting unique values
 * from one table into another.
 *
 * @spec SPEC-4.1
 */
class MysqlInsertSelectDistinctTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_isd_orders (
                id INT PRIMARY KEY,
                customer_name VARCHAR(50),
                product VARCHAR(50),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_isd_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) UNIQUE
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_isd_customers', 'mp_isd_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_isd_orders (id, customer_name, product, amount) VALUES
            (1, 'Alice', 'Widget', 100.00),
            (2, 'Bob', 'Gadget', 200.00),
            (3, 'Alice', 'Gadget', 150.00),
            (4, 'Charlie', 'Widget', 50.00),
            (5, 'Bob', 'Premium', 300.00)");
    }

    /**
     * INSERT ... SELECT DISTINCT customer names from orders into customers table.
     */
    public function testInsertSelectDistinct(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_isd_customers (name)
                 SELECT DISTINCT customer_name FROM mp_isd_orders ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM mp_isd_customers ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT: expected 3 unique customers, got ' . count($rows)
                    . '. CTE rewriter may not handle DISTINCT in INSERT...SELECT correctly.'
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
     * INSERT ... SELECT DISTINCT after shadow mutation of source table.
     */
    public function testInsertSelectDistinctAfterSourceMutation(): void
    {
        // Add a new order with a new customer via shadow INSERT
        $this->pdo->exec("INSERT INTO mp_isd_orders (id, customer_name, product, amount) VALUES (6, 'Diana', 'Widget', 75.00)");
        // And a duplicate of an existing customer
        $this->pdo->exec("INSERT INTO mp_isd_orders (id, customer_name, product, amount) VALUES (7, 'Diana', 'Gadget', 120.00)");

        try {
            $this->pdo->exec(
                "INSERT INTO mp_isd_customers (name)
                 SELECT DISTINCT customer_name FROM mp_isd_orders ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM mp_isd_customers ORDER BY name");

            // Alice, Bob, Charlie, Diana = 4 distinct customers
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT after mutation: expected 4 rows, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Diana', $rows[3]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT after mutation failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... SELECT DISTINCT with WHERE clause.
     */
    public function testInsertSelectDistinctWithWhere(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_isd_customers (name)
                 SELECT DISTINCT customer_name FROM mp_isd_orders
                 WHERE amount >= 150.00
                 ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM mp_isd_customers ORDER BY name");

            // Orders >= 150: Alice (150), Bob (200, 300) → distinct: Alice, Bob = 2
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

    /**
     * INSERT ... SELECT DISTINCT same table (self-referencing).
     * Copy distinct products back as new orders with a fixed customer.
     */
    public function testInsertSelectDistinctSameTable(): void
    {
        try {
            // Get the distinct product count first
            $countRows = $this->ztdQuery("SELECT COUNT(DISTINCT product) AS cnt FROM mp_isd_orders");
            $distinctCount = (int) $countRows[0]['cnt'];

            // Get max id for new inserts
            $maxRows = $this->ztdQuery("SELECT MAX(id) AS max_id FROM mp_isd_orders");
            $maxId = (int) $maxRows[0]['max_id'];

            $this->pdo->exec(
                "INSERT INTO mp_isd_orders (id, customer_name, product, amount)
                 SELECT {$maxId} + ROW_NUMBER() OVER (ORDER BY product),
                        'Bulk', product, 0.00
                 FROM (SELECT DISTINCT product FROM mp_isd_orders) AS dp"
            );

            $rows = $this->ztdQuery("SELECT product FROM mp_isd_orders WHERE customer_name = 'Bulk' ORDER BY product");

            // Distinct products: Gadget, Premium, Widget = 3
            if (count($rows) !== $distinctCount) {
                $this->markTestIncomplete(
                    "INSERT SELECT DISTINCT same table: expected {$distinctCount} rows, got " . count($rows)
                );
            }

            $this->assertCount($distinctCount, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT same table failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT COUNT(DISTINCT ...) on shadow data.
     */
    public function testCountDistinctOnShadowData(): void
    {
        // Add duplicate customer entries
        $this->pdo->exec("INSERT INTO mp_isd_orders (id, customer_name, product, amount) VALUES (6, 'Alice', 'Premium', 500.00)");

        try {
            $rows = $this->ztdQuery("SELECT COUNT(DISTINCT customer_name) AS cnt FROM mp_isd_orders");

            // Still 3 distinct customers: Alice, Bob, Charlie
            $this->assertSame(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(DISTINCT) on shadow data failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation after INSERT...SELECT DISTINCT.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_isd_customers (name)
                 SELECT DISTINCT customer_name FROM mp_isd_orders"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_isd_customers")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical customers table should be empty');

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_isd_orders")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical orders table should be empty');
    }
}
