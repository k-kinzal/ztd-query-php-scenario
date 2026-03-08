<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL-specific advanced features with ZTD shadow store:
 * GROUP_CONCAT with ORDER BY, IF/IFNULL, date functions, multi-column IN.
 * @spec pending
 */
class MysqlAdvancedPlatformTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE madv_users (id INT PRIMARY KEY, name VARCHAR(50), department VARCHAR(20), salary DECIMAL(10,2))',
            'CREATE TABLE madv_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(20), created_at DATE)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['madv_orders', 'madv_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO madv_users VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->pdo->exec("INSERT INTO madv_users VALUES (2, 'Bob', 'Marketing', 75000)");
        $this->pdo->exec("INSERT INTO madv_users VALUES (3, 'Charlie', 'Engineering', 85000)");
        $this->pdo->exec("INSERT INTO madv_users VALUES (4, 'Diana', 'Marketing', 80000)");
        $this->pdo->exec("INSERT INTO madv_orders VALUES (1, 1, 100.00, 'completed', '2024-01-15')");
        $this->pdo->exec("INSERT INTO madv_orders VALUES (2, 1, 200.00, 'pending', '2024-02-15')");
        $this->pdo->exec("INSERT INTO madv_orders VALUES (3, 2, 150.00, 'completed', '2024-01-20')");
        $this->pdo->exec("INSERT INTO madv_orders VALUES (4, 3, 300.00, 'completed', '2024-03-01')");
    }

    public function testGroupConcatWithOrderBy(): void
    {
        $stmt = $this->pdo->query(
            "SELECT department, GROUP_CONCAT(name ORDER BY name SEPARATOR ', ') as members
             FROM madv_users
             GROUP BY department
             ORDER BY department"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('Alice, Charlie', $rows[0]['members']);
    }

    public function testIfAndIfnullFunctions(): void
    {
        $this->pdo->exec('INSERT INTO madv_users VALUES (5, NULL, NULL, NULL)');

        $stmt = $this->pdo->query(
            "SELECT
                IF(salary > 80000, 'High', 'Low') as salary_tier,
                IFNULL(name, 'Anonymous') as display_name
             FROM madv_users WHERE id = 5"
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Low', $row['salary_tier']);
        $this->assertSame('Anonymous', $row['display_name']);
    }

    public function testConcatWsFunctionWithNulls(): void
    {
        $this->pdo->exec("INSERT INTO madv_users VALUES (6, 'Eve', NULL, 60000)");

        $stmt = $this->pdo->query(
            "SELECT CONCAT_WS(' - ', name, department, 'Staff') as label FROM madv_users WHERE id = 6"
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // CONCAT_WS skips NULLs
        $this->assertSame('Eve - Staff', $row['label']);
    }

    public function testSubqueryWithMultipleAggregates(): void
    {
        $stmt = $this->pdo->query(
            'SELECT u.name,
                    (SELECT COUNT(*) FROM madv_orders o WHERE o.user_id = u.id) as order_count,
                    (SELECT SUM(amount) FROM madv_orders o WHERE o.user_id = u.id) as total_spent
             FROM madv_users u
             WHERE u.id IN (1, 2)
             ORDER BY u.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['order_count']);
        $this->assertEquals(300.00, (float) $rows[0]['total_spent']);
    }

    public function testUpdateWithCaseAndArithmetic(): void
    {
        // Give 10% raise to Engineering, 5% to Marketing
        $this->pdo->exec(
            "UPDATE madv_users SET salary = salary * CASE department
                WHEN 'Engineering' THEN 1.10
                WHEN 'Marketing' THEN 1.05
                ELSE 1.00
            END"
        );

        $stmt = $this->pdo->query('SELECT name, salary FROM madv_users ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Alice: 90000 * 1.10 = 99000
        $this->assertEqualsWithDelta(99000, (float) $rows[0]['salary'], 1);
        // Bob: 75000 * 1.05 = 78750
        $this->assertEqualsWithDelta(78750, (float) $rows[1]['salary'], 1);
    }

    public function testPreparedJoinWithAggregation(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT u.department,
                    COUNT(o.id) as order_count,
                    SUM(o.amount) as total_amount
             FROM madv_users u
             LEFT JOIN madv_orders o ON u.id = o.user_id AND o.status = ?
             GROUP BY u.department
             ORDER BY u.department"
        );
        $stmt->execute(['completed']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
    }

    public function testReverseLpadFunctions(): void
    {
        $stmt = $this->pdo->query(
            "SELECT REVERSE(name) as reversed, LPAD(CAST(id AS CHAR), 5, '0') as padded_id
             FROM madv_users WHERE id = 1"
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ecilA', $row['reversed']);
        $this->assertSame('00001', $row['padded_id']);
    }
}
