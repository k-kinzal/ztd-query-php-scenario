<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use Tests\Support\MySQLContainer;

/** @spec SPEC-3.3 */
class ComplexQueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), department VARCHAR(255))',
            'CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(50))',
            'CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, product VARCHAR(255), qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['order_items', 'orders', 'users'];
    }


    public function testJoinWithShadowData(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (10, 1, 99.99, 'completed')");

        $result = $this->mysqli->query(
            'SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 1'
        );
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
        $this->assertSame('99.99', $row['amount']);
    }

    public function testLeftJoinWithShadowData(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (10, 1, 50.00, 'completed')");

        $result = $this->mysqli->query(
            'SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('50.00', $rows[0]['amount']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertNull($rows[1]['amount']);
    }

    public function testCountAggregation(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM users');
        $row = $result->fetch_assoc();

        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testGroupByWithCount(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $result = $this->mysqli->query(
            'SELECT department, COUNT(*) AS cnt FROM users GROUP BY department ORDER BY department'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertSame('Sales', $rows[1]['department']);
        $this->assertSame(1, (int) $rows[1]['cnt']);
    }

    public function testSumAggregation(): void
    {
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.50, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $result = $this->mysqli->query("SELECT SUM(amount) AS total FROM orders WHERE status = 'completed'");
        $row = $result->fetch_assoc();

        $this->assertSame('300.50', $row['total']);
    }

    public function testOrderByAndLimit(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Engineering')");

        $result = $this->mysqli->query('SELECT name FROM users ORDER BY name ASC LIMIT 2');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testLimitWithOffset(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'A')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'B')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'C')");

        $result = $this->mysqli->query('SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testSubqueryInWhere(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (10, 1, 99.99, 'completed')");

        $result = $this->mysqli->query(
            'SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testHavingClause(): void
    {
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'completed')");

        $result = $this->mysqli->query(
            'SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING total > 100 ORDER BY user_id'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
        $this->assertSame('300.00', $rows[0]['total']);
    }

    public function testDistinctSelect(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $result = $this->mysqli->query('SELECT DISTINCT department FROM users ORDER BY department');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('Sales', $rows[1]['department']);
    }

    public function testJoinWithAggregationOnShadowData(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $result = $this->mysqli->query(
            'SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total '
            . 'FROM users u INNER JOIN orders o ON u.id = o.user_id '
            . 'GROUP BY u.id, u.name ORDER BY u.name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertSame('300.00', $rows[0]['total']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(1, (int) $rows[1]['order_count']);
        $this->assertSame('50.00', $rows[1]['total']);
    }

    public function testUpdateWithJoinedCondition(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'pending')");

        $this->mysqli->query(
            "UPDATE orders SET status = 'completed' WHERE user_id IN (SELECT id FROM users WHERE department = 'Engineering')"
        );

        $result = $this->mysqli->query('SELECT status FROM orders WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('completed', $row['status']);
    }

    public function testDeleteWithSubquery(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 2, 50.00, 'completed')");

        $this->mysqli->query(
            "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE department = 'Sales')"
        );

        $result = $this->mysqli->query('SELECT * FROM orders');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testSelfJoin(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $result = $this->mysqli->query(
            'SELECT a.name AS name1, b.name AS name2 '
            . 'FROM users a INNER JOIN users b ON a.department = b.department AND a.id < b.id '
            . 'ORDER BY a.name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name1']);
        $this->assertSame('Bob', $rows[0]['name2']);
    }

    public function testUnionQuery(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");

        $result = $this->mysqli->query(
            "SELECT name FROM users WHERE department = 'Engineering' "
            . "UNION SELECT name FROM users WHERE department = 'Sales' "
            . 'ORDER BY name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testMinMaxAggregation(): void
    {
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $result = $this->mysqli->query('SELECT MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM orders');
        $row = $result->fetch_assoc();

        $this->assertSame('50.00', $row['min_amt']);
        $this->assertSame('200.00', $row['max_amt']);
    }

    public function testCorrelatedSubquery(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->mysqli->query("INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");

        $result = $this->mysqli->query(
            'SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count '
            . 'FROM users u ORDER BY u.id'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(0, (int) $rows[1]['order_count']);
    }

    public function testZtdSelectReadsOnlyFromShadowStore(): void
    {
        // Insert physical data via raw connection
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query("INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $raw->close();

        // Physical data is visible with ZTD disabled (passthrough)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM users');
        $this->assertSame(1, $result->num_rows, 'Physical data visible with ZTD disabled');
        $this->mysqli->enableZtd();

        // With ZTD enabled, SELECT reads only from the shadow store.
        // Physical table data is NOT merged — the shadow store is an
        // independent layer that replaces the physical table entirely.
        $result = $this->mysqli->query('SELECT * FROM users');
        $this->assertSame(0, $result->num_rows,
            'ZTD SELECT reads only from shadow store; physical data is not visible');
    }
}
