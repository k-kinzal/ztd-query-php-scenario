<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests complex query patterns in ZTD mode on MySQL via PDO:
 * joins, aggregation, subqueries, self-joins, UNION.
 * @spec SPEC-3.3
 */
class MysqlComplexQueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_cq_users (id INT PRIMARY KEY, name VARCHAR(255), department VARCHAR(255))',
            'CREATE TABLE mysql_cq_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_cq_orders', 'mysql_cq_users'];
    }


    public function testJoinWithShadowData(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (10, 1, 99.99, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT u.name, o.amount FROM mysql_cq_users u INNER JOIN mysql_cq_orders o ON u.id = o.user_id WHERE u.id = 1'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(99.99, (float) $rows[0]['amount']);
    }

    public function testLeftJoinWithShadowData(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (10, 1, 50.00, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT u.name, o.amount FROM mysql_cq_users u LEFT JOIN mysql_cq_orders o ON u.id = o.user_id ORDER BY u.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertNotNull($rows[0]['amount']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertNull($rows[1]['amount']);
    }

    public function testGroupByWithCount(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $stmt = $this->pdo->query(
            'SELECT department, COUNT(*) AS cnt FROM mysql_cq_users GROUP BY department ORDER BY department'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertSame('Sales', $rows[1]['department']);
        $this->assertSame(1, (int) $rows[1]['cnt']);
    }

    public function testSumAggregation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (2, 1, 200.50, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $stmt = $this->pdo->query("SELECT SUM(amount) AS total FROM mysql_cq_orders WHERE status = 'completed'");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(300.5, (float) $rows[0]['total']);
    }

    public function testOrderByAndLimit(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (3, 'Charlie', 'Engineering')");

        $stmt = $this->pdo->query('SELECT name FROM mysql_cq_users ORDER BY name ASC LIMIT 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testSubqueryInWhere(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (10, 1, 99.99, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT name FROM mysql_cq_users WHERE id IN (SELECT user_id FROM mysql_cq_orders)'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testHavingClause(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'completed')");

        // MySQL requires using the aggregate expression in HAVING (not alias)
        $stmt = $this->pdo->query(
            'SELECT user_id, SUM(amount) AS total FROM mysql_cq_orders GROUP BY user_id HAVING SUM(amount) > 100 ORDER BY user_id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
        $this->assertSame(300.0, (float) $rows[0]['total']);
    }

    public function testDistinctSelect(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $stmt = $this->pdo->query('SELECT DISTINCT department FROM mysql_cq_users ORDER BY department');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('Sales', $rows[1]['department']);
    }

    public function testSelfJoin(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $stmt = $this->pdo->query(
            'SELECT a.name AS name1, b.name AS name2 '
            . 'FROM mysql_cq_users a INNER JOIN mysql_cq_users b ON a.department = b.department AND a.id < b.id '
            . 'ORDER BY a.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name1']);
        $this->assertSame('Bob', $rows[0]['name2']);
    }

    public function testUnionQuery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Sales')");

        $stmt = $this->pdo->query(
            "SELECT name FROM mysql_cq_users WHERE department = 'Engineering' "
            . "UNION SELECT name FROM mysql_cq_users WHERE department = 'Sales' "
            . 'ORDER BY name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testCountAggregation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (3, 'Charlie', 'Sales')");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mysql_cq_users');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    public function testLimitWithOffset(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'A')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'B')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (3, 'Charlie', 'C')");

        $stmt = $this->pdo->query('SELECT name FROM mysql_cq_users ORDER BY id LIMIT 2 OFFSET 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testJoinWithAggregationOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $stmt = $this->pdo->query(
            'SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total '
            . 'FROM mysql_cq_users u INNER JOIN mysql_cq_orders o ON u.id = o.user_id '
            . 'GROUP BY u.id, u.name ORDER BY u.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertSame(300.0, (float) $rows[0]['total']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(1, (int) $rows[1]['order_count']);
        $this->assertSame(50.0, (float) $rows[1]['total']);
    }

    public function testCorrelatedSubquery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");

        $stmt = $this->pdo->query(
            'SELECT u.name, (SELECT COUNT(*) FROM mysql_cq_orders o WHERE o.user_id = u.id) AS order_count '
            . 'FROM mysql_cq_users u ORDER BY u.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(0, (int) $rows[1]['order_count']);
    }

    public function testUpdateWithSubquery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'pending')");

        $this->pdo->exec(
            "UPDATE mysql_cq_orders SET status = 'completed' WHERE user_id IN (SELECT id FROM mysql_cq_users WHERE department = 'Engineering')"
        );

        $stmt = $this->pdo->query('SELECT status FROM mysql_cq_orders WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('completed', $rows[0]['status']);
    }

    public function testDeleteWithSubquery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (2, 2, 50.00, 'completed')");

        $this->pdo->exec(
            "DELETE FROM mysql_cq_orders WHERE user_id IN (SELECT id FROM mysql_cq_users WHERE department = 'Sales')"
        );

        $stmt = $this->pdo->query('SELECT * FROM mysql_cq_orders');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testMinMaxAggregation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (1, 1, 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (2, 1, 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_cq_orders (id, user_id, amount, status) VALUES (3, 2, 50.00, 'pending')");

        $stmt = $this->pdo->query('SELECT MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM mysql_cq_orders');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(50.0, (float) $rows[0]['min_amt']);
        $this->assertSame(200.0, (float) $rows[0]['max_amt']);
    }

    public function testZtdSelectReadsOnlyFromShadowStore(): void
    {
        // Insert physical data via raw connection
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec("INSERT INTO mysql_cq_users (id, name, department) VALUES (1, 'Alice', 'Engineering')");

        // Physical data is visible with ZTD disabled (passthrough)
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_cq_users');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC),
            'Physical data visible with ZTD disabled');
        $this->pdo->enableZtd();

        // With ZTD enabled, SELECT reads only from the shadow store.
        $stmt = $this->pdo->query('SELECT * FROM mysql_cq_users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC),
            'ZTD SELECT reads only from shadow store; physical data is not visible');
    }
}
