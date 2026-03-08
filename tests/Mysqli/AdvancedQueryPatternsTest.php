<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests advanced SQL query patterns in ZTD mode via MySQLi:
 * CASE, LIKE, IN, BETWEEN, EXISTS, COALESCE, window functions.
 * @spec SPEC-3.3
 */
class AdvancedQueryPatternsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE adv_employees (id INT PRIMARY KEY, name VARCHAR(255), department VARCHAR(255), salary DECIMAL(10,2))',
            'CREATE TABLE adv_emp (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE adv_tasks (id INT PRIMARY KEY, emp_id INT, task VARCHAR(255))',
            'CREATE TABLE adv_emp2 (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE adv_tasks2 (id INT PRIMARY KEY, emp_id INT, task VARCHAR(255))',
            'CREATE TABLE adv_contact (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), phone VARCHAR(255))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['adv_employees', 'adv_tasks', 'adv_emp', 'adv_tasks2', 'adv_emp2', 'adv_contact'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO adv_employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->mysqli->query("INSERT INTO adv_employees (id, name, department, salary) VALUES (2, 'Bob', 'Sales', 60000)");
        $this->mysqli->query("INSERT INTO adv_employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 110000)");
        $this->mysqli->query("INSERT INTO adv_employees (id, name, department, salary) VALUES (4, 'Diana', 'Marketing', 75000)");
        $this->mysqli->query("INSERT INTO adv_employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 55000)");
    }

    public function testCaseExpression(): void
    {
        $result = $this->mysqli->query("
            SELECT name,
                CASE
                    WHEN salary >= 100000 THEN 'high'
                    WHEN salary >= 70000 THEN 'medium'
                    ELSE 'low'
                END AS salary_band
            FROM adv_employees ORDER BY id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertSame('medium', $rows[0]['salary_band']); // Alice 90k
        $this->assertSame('low', $rows[1]['salary_band']);     // Bob 60k
        $this->assertSame('high', $rows[2]['salary_band']);    // Charlie 110k
    }

    public function testLikeClause(): void
    {
        $result = $this->mysqli->query("SELECT name FROM adv_employees WHERE name LIKE 'A%' ORDER BY name");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testLikeClauseWildcard(): void
    {
        $result = $this->mysqli->query("SELECT name FROM adv_employees WHERE name LIKE '%li%' ORDER BY name");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows); // Alice, Charlie
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testInClause(): void
    {
        $result = $this->mysqli->query("SELECT name FROM adv_employees WHERE department IN ('Engineering', 'Marketing') ORDER BY name");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
        $this->assertSame('Diana', $rows[2]['name']);
    }

    public function testBetweenClause(): void
    {
        $result = $this->mysqli->query("SELECT name FROM adv_employees WHERE salary BETWEEN 60000 AND 80000 ORDER BY name");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
    }

    public function testExistsSubquery(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS adv_tasks');
        $raw->query('DROP TABLE IF EXISTS adv_emp');
        $raw->query('CREATE TABLE adv_emp (id INT PRIMARY KEY, name VARCHAR(255))');
        $raw->query('CREATE TABLE adv_tasks (id INT PRIMARY KEY, emp_id INT, task VARCHAR(255))');
        $raw->close();

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $mysqli->query("INSERT INTO adv_emp (id, name) VALUES (1, 'Alice')");
        $mysqli->query("INSERT INTO adv_emp (id, name) VALUES (2, 'Bob')");
        $mysqli->query("INSERT INTO adv_tasks (id, emp_id, task) VALUES (1, 1, 'Review code')");

        $result = $mysqli->query("SELECT name FROM adv_emp e WHERE EXISTS (SELECT 1 FROM adv_tasks t WHERE t.emp_id = e.id)");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $mysqli->close();

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS adv_tasks');
        $raw->query('DROP TABLE IF EXISTS adv_emp');
        $raw->close();
    }

    public function testNotExistsSubquery(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS adv_tasks2');
        $raw->query('DROP TABLE IF EXISTS adv_emp2');
        $raw->query('CREATE TABLE adv_emp2 (id INT PRIMARY KEY, name VARCHAR(255))');
        $raw->query('CREATE TABLE adv_tasks2 (id INT PRIMARY KEY, emp_id INT, task VARCHAR(255))');
        $raw->close();

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $mysqli->query("INSERT INTO adv_emp2 (id, name) VALUES (1, 'Alice')");
        $mysqli->query("INSERT INTO adv_emp2 (id, name) VALUES (2, 'Bob')");
        $mysqli->query("INSERT INTO adv_tasks2 (id, emp_id, task) VALUES (1, 1, 'Review code')");

        $result = $mysqli->query("SELECT name FROM adv_emp2 e WHERE NOT EXISTS (SELECT 1 FROM adv_tasks2 t WHERE t.emp_id = e.id)");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);

        $mysqli->close();

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS adv_tasks2');
        $raw->query('DROP TABLE IF EXISTS adv_emp2');
        $raw->close();
    }

    public function testCoalesce(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS adv_contact');
        $raw->query('CREATE TABLE adv_contact (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), phone VARCHAR(255))');
        $raw->close();

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $mysqli->query("INSERT INTO adv_contact (id, name, email, phone) VALUES (1, 'Alice', 'alice@example.com', NULL)");
        $mysqli->query("INSERT INTO adv_contact (id, name, email, phone) VALUES (2, 'Bob', NULL, '555-0100')");

        $result = $mysqli->query("SELECT name, COALESCE(email, phone, 'N/A') AS contact_info FROM adv_contact ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertSame('alice@example.com', $rows[0]['contact_info']);
        $this->assertSame('555-0100', $rows[1]['contact_info']);

        $mysqli->close();

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS adv_contact');
        $raw->close();
    }

    public function testWindowFunctionRowNumber(): void
    {
        $result = $this->mysqli->query("
            SELECT name, department, salary,
                ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
            FROM adv_employees ORDER BY department, rank_in_dept
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(5, $rows);

        // Engineering: Charlie (110k) rank 1, Alice (90k) rank 2
        $engineering = array_values(array_filter($rows, fn($r) => $r['department'] === 'Engineering'));
        $this->assertSame('Charlie', $engineering[0]['name']);
        $this->assertSame(1, (int) $engineering[0]['rank_in_dept']);
        $this->assertSame('Alice', $engineering[1]['name']);
        $this->assertSame(2, (int) $engineering[1]['rank_in_dept']);
    }

    public function testWindowFunctionSum(): void
    {
        $result = $this->mysqli->query("
            SELECT name, department, salary,
                SUM(salary) OVER (PARTITION BY department) AS dept_total
            FROM adv_employees WHERE department = 'Engineering' ORDER BY name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('200000.00', $rows[0]['dept_total']);
        $this->assertSame('200000.00', $rows[1]['dept_total']);
    }

    public function testUpdateWithCaseExpression(): void
    {
        $this->mysqli->query("
            UPDATE adv_employees SET salary = CASE
                WHEN department = 'Engineering' THEN salary * 1.1
                ELSE salary * 1.05
            END
        ");

        $result = $this->mysqli->query("SELECT name, salary FROM adv_employees ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertSame(99000.0, (float) $rows[0]['salary']);  // Alice: 90000 * 1.1
        $this->assertSame(63000.0, (float) $rows[1]['salary']);  // Bob: 60000 * 1.05
        $this->assertSame(121000.0, (float) $rows[2]['salary']); // Charlie: 110000 * 1.1
    }
}
