<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests prepared statements with parameters in HAVING, GROUP BY, and ORDER BY on PostgreSQL PDO.
 * @spec SPEC-3.2
 */
class PostgresPreparedAggregateParamsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pap_orders (id INT PRIMARY KEY, customer VARCHAR(50), product VARCHAR(50), amount DECIMAL(10,2), status VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['pap_orders'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pap_orders (id, customer, product, amount, status) VALUES (1, 'Alice', 'Widget', 50.0, 'completed')");
        $this->pdo->exec("INSERT INTO pap_orders (id, customer, product, amount, status) VALUES (2, 'Alice', 'Gadget', 30.0, 'completed')");
        $this->pdo->exec("INSERT INTO pap_orders (id, customer, product, amount, status) VALUES (3, 'Bob', 'Widget', 120.0, 'completed')");
        $this->pdo->exec("INSERT INTO pap_orders (id, customer, product, amount, status) VALUES (4, 'Bob', 'Doohickey', 15.0, 'pending')");
        $this->pdo->exec("INSERT INTO pap_orders (id, customer, product, amount, status) VALUES (5, 'Charlie', 'Gadget', 30.0, 'completed')");
    }

    public function testHavingWithParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT customer, COUNT(*) AS cnt FROM pap_orders GROUP BY customer HAVING COUNT(*) >= ?');
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $customers = array_column($rows, 'customer');
        sort($customers);
        $this->assertSame(['Alice', 'Bob'], $customers);
    }

    public function testHavingSumWithParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT customer, SUM(amount) AS total FROM pap_orders GROUP BY customer HAVING SUM(amount) > ?');
        $stmt->execute([80.0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
    }

    public function testCombinedWhereGroupByHavingParams(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT customer, SUM(amount) AS total '
            . 'FROM pap_orders '
            . 'WHERE status = ? '
            . 'GROUP BY customer '
            . 'HAVING SUM(amount) >= ? '
            . 'ORDER BY total DESC'
        );
        $stmt->execute(['completed', 50.0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
        $this->assertSame('Alice', $rows[1]['customer']);
    }

    public function testGroupByWithWhereParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT customer, SUM(amount) AS total FROM pap_orders WHERE status = ? GROUP BY customer ORDER BY total DESC');
        $stmt->execute(['completed']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
    }
}
