<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL-specific advanced features: GROUP_CONCAT, IF/IFNULL, CASE UPDATE, subqueries via MySQLi.
 *
 * Cross-platform parity with MysqlAdvancedPlatformTest (PDO).
 * @spec pending
 */
class AdvancedPlatformTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_adv_users (id INT PRIMARY KEY, name VARCHAR(50), department VARCHAR(20), salary DECIMAL(10,2))',
            'CREATE TABLE mi_adv_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(20), created_at DATE)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_adv_orders', 'mi_adv_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_adv_users VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->mysqli->query("INSERT INTO mi_adv_users VALUES (2, 'Bob', 'Marketing', 75000)");
        $this->mysqli->query("INSERT INTO mi_adv_users VALUES (3, 'Charlie', 'Engineering', 85000)");
        $this->mysqli->query("INSERT INTO mi_adv_users VALUES (4, 'Diana', 'Marketing', 80000)");
        $this->mysqli->query("INSERT INTO mi_adv_orders VALUES (1, 1, 100.00, 'completed', '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_adv_orders VALUES (2, 1, 200.00, 'pending', '2024-02-15')");
        $this->mysqli->query("INSERT INTO mi_adv_orders VALUES (3, 2, 150.00, 'completed', '2024-01-20')");
        $this->mysqli->query("INSERT INTO mi_adv_orders VALUES (4, 3, 300.00, 'completed', '2024-03-01')");
    }

    public function testGroupConcatWithOrderBy(): void
    {
        $result = $this->mysqli->query(
            "SELECT department, GROUP_CONCAT(name ORDER BY name SEPARATOR ', ') as members
             FROM mi_adv_users
             GROUP BY department
             ORDER BY department"
        );
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('Alice, Charlie', $rows[0]['members']);
    }

    public function testIfAndIfnullFunctions(): void
    {
        $this->mysqli->query('INSERT INTO mi_adv_users VALUES (5, NULL, NULL, NULL)');

        $result = $this->mysqli->query(
            "SELECT
                IF(salary > 80000, 'High', 'Low') as salary_tier,
                IFNULL(name, 'Anonymous') as display_name
             FROM mi_adv_users WHERE id = 5"
        );
        $row = $result->fetch_assoc();
        $this->assertSame('Low', $row['salary_tier']);
        $this->assertSame('Anonymous', $row['display_name']);
    }

    public function testConcatWsFunctionWithNulls(): void
    {
        $this->mysqli->query("INSERT INTO mi_adv_users VALUES (6, 'Eve', NULL, 60000)");

        $result = $this->mysqli->query(
            "SELECT CONCAT_WS(' - ', name, department, 'Staff') as label FROM mi_adv_users WHERE id = 6"
        );
        // CONCAT_WS skips NULLs
        $this->assertSame('Eve - Staff', $result->fetch_assoc()['label']);
    }

    public function testSubqueryWithMultipleAggregates(): void
    {
        $result = $this->mysqli->query(
            'SELECT u.name,
                    (SELECT COUNT(*) FROM mi_adv_orders o WHERE o.user_id = u.id) as order_count,
                    (SELECT SUM(amount) FROM mi_adv_orders o WHERE o.user_id = u.id) as total_spent
             FROM mi_adv_users u
             WHERE u.id IN (1, 2)
             ORDER BY u.name'
        );
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['order_count']);
        $this->assertEquals(300.00, (float) $rows[0]['total_spent']);
    }

    public function testUpdateWithCaseAndArithmetic(): void
    {
        $this->mysqli->query(
            "UPDATE mi_adv_users SET salary = salary * CASE department
                WHEN 'Engineering' THEN 1.10
                WHEN 'Marketing' THEN 1.05
                ELSE 1.00
            END"
        );

        $result = $this->mysqli->query('SELECT name, salary FROM mi_adv_users ORDER BY id');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row;
        }

        // Alice: 90000 * 1.10 = 99000
        $this->assertEqualsWithDelta(99000, (float) $rows[0]['salary'], 1);
        // Bob: 75000 * 1.05 = 78750
        $this->assertEqualsWithDelta(78750, (float) $rows[1]['salary'], 1);
    }

    public function testPreparedJoinWithAggregation(): void
    {
        $stmt = $this->mysqli->prepare(
            "SELECT u.department,
                    COUNT(o.id) as order_count,
                    SUM(o.amount) as total_amount
             FROM mi_adv_users u
             LEFT JOIN mi_adv_orders o ON u.id = o.user_id AND o.status = ?
             GROUP BY u.department
             ORDER BY u.department"
        );
        $status = 'completed';
        $stmt->bind_param('s', $status);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
    }

    public function testReverseLpadFunctions(): void
    {
        $result = $this->mysqli->query(
            "SELECT REVERSE(name) as reversed, LPAD(CAST(id AS CHAR), 5, '0') as padded_id
             FROM mi_adv_users WHERE id = 1"
        );
        $row = $result->fetch_assoc();
        $this->assertSame('ecilA', $row['reversed']);
        $this->assertSame('00001', $row['padded_id']);
    }
}
