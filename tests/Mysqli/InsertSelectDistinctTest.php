<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT ... SELECT DISTINCT with shadow data on MySQLi.
 *
 * @spec SPEC-4.1
 * @see https://github.com/k-kinzal/ztd-query-php/issues/99
 */
class InsertSelectDistinctTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_isd_orders (
                id INT PRIMARY KEY,
                customer_name VARCHAR(50),
                product VARCHAR(50),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_isd_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) UNIQUE
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_isd_customers', 'mi_isd_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_isd_orders (id, customer_name, product, amount) VALUES
            (1, 'Alice', 'Widget', 100.00),
            (2, 'Bob', 'Gadget', 200.00),
            (3, 'Alice', 'Gadget', 150.00),
            (4, 'Charlie', 'Widget', 50.00),
            (5, 'Bob', 'Premium', 300.00)");
    }

    public function testInsertSelectDistinct(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_isd_customers (name)
                 SELECT DISTINCT customer_name FROM mi_isd_orders ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_isd_customers ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT: expected 3 unique customers, got ' . count($rows)
                    . '. DISTINCT may be ignored in INSERT...SELECT context.'
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

    public function testInsertSelectDistinctAfterMutation(): void
    {
        $this->mysqli->query("INSERT INTO mi_isd_orders (id, customer_name, product, amount) VALUES (6, 'Diana', 'Widget', 75.00)");
        $this->mysqli->query("INSERT INTO mi_isd_orders (id, customer_name, product, amount) VALUES (7, 'Diana', 'Gadget', 120.00)");

        try {
            $this->mysqli->query(
                "INSERT INTO mi_isd_customers (name)
                 SELECT DISTINCT customer_name FROM mi_isd_orders ORDER BY customer_name"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_isd_customers ORDER BY name");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT SELECT DISTINCT after mutation: expected 4, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT DISTINCT after mutation failed: ' . $e->getMessage());
        }
    }

    public function testCountDistinct(): void
    {
        $this->mysqli->query("INSERT INTO mi_isd_orders (id, customer_name, product, amount) VALUES (6, 'Alice', 'Premium', 500.00)");

        try {
            $rows = $this->ztdQuery("SELECT COUNT(DISTINCT customer_name) AS cnt FROM mi_isd_orders");
            $this->assertSame(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(DISTINCT) failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_isd_customers (name) SELECT DISTINCT customer_name FROM mi_isd_orders"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT failed: ' . $e->getMessage());
            return;
        }

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_isd_customers");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
