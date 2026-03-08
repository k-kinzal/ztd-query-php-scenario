<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared statements with parameters in HAVING, GROUP BY, and ORDER BY on MySQLi.
 * @spec pending
 */
class PreparedAggregateParamsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_pap_orders (id INT PRIMARY KEY, customer VARCHAR(50), amount DECIMAL(10,2), status VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['mi_pap_orders'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_pap_orders (id, customer, amount, status) VALUES (1, 'Alice', 50.0, 'completed')");
        $this->mysqli->query("INSERT INTO mi_pap_orders (id, customer, amount, status) VALUES (2, 'Alice', 30.0, 'completed')");
        $this->mysqli->query("INSERT INTO mi_pap_orders (id, customer, amount, status) VALUES (3, 'Bob', 120.0, 'completed')");
        $this->mysqli->query("INSERT INTO mi_pap_orders (id, customer, amount, status) VALUES (4, 'Bob', 15.0, 'pending')");
        $this->mysqli->query("INSERT INTO mi_pap_orders (id, customer, amount, status) VALUES (5, 'Charlie', 30.0, 'completed')");
    }

    public function testHavingWithParam(): void
    {
        $stmt = $this->mysqli->prepare('SELECT customer, COUNT(*) AS cnt FROM mi_pap_orders GROUP BY customer HAVING COUNT(*) >= ?');
        $min = 2;
        $stmt->bind_param('i', $min);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $customers = array_column($rows, 'customer');
        sort($customers);
        $this->assertSame(['Alice', 'Bob'], $customers);
    }

    public function testHavingSumWithParam(): void
    {
        $stmt = $this->mysqli->prepare('SELECT customer, SUM(amount) AS total FROM mi_pap_orders GROUP BY customer HAVING SUM(amount) > ?');
        $threshold = 80.0;
        $stmt->bind_param('d', $threshold);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
    }

    public function testGroupByWithWhereParam(): void
    {
        $stmt = $this->mysqli->prepare('SELECT customer, SUM(amount) AS total FROM mi_pap_orders WHERE status = ? GROUP BY customer ORDER BY total DESC');
        $status = 'completed';
        $stmt->bind_param('s', $status);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
    }
}
