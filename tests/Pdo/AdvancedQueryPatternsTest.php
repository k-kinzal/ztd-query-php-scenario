<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests advanced SQL query patterns in ZTD mode:
 * CASE, LIKE, IN, BETWEEN, EXISTS, COALESCE, window functions.
 */
class AdvancedQueryPatternsTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL)');

        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Sales', 60000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 110000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (4, 'Diana', 'Marketing', 75000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 55000)");
    }

    public function testCaseExpression(): void
    {
        $stmt = $this->pdo->query("
            SELECT name,
                CASE
                    WHEN salary >= 100000 THEN 'high'
                    WHEN salary >= 70000 THEN 'medium'
                    ELSE 'low'
                END AS salary_band
            FROM employees ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('medium', $rows[0]['salary_band']); // Alice 90k
        $this->assertSame('low', $rows[1]['salary_band']);     // Bob 60k
        $this->assertSame('high', $rows[2]['salary_band']);    // Charlie 110k
    }

    public function testLikeClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM employees WHERE name LIKE 'A%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testLikeClauseWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM employees WHERE name LIKE '%li%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows); // Alice, Charlie
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testInClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM employees WHERE department IN ('Engineering', 'Marketing') ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
        $this->assertSame('Diana', $rows[2]['name']);
    }

    public function testBetweenClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM employees WHERE salary BETWEEN 60000 AND 80000 ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
    }

    public function testExistsSubquery(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT)');
        $raw->exec('CREATE TABLE tasks (id INTEGER PRIMARY KEY, emp_id INTEGER, task TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO emp (id, name) VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO emp (id, name) VALUES (2, 'Bob')");
        $pdo->exec("INSERT INTO tasks (id, emp_id, task) VALUES (1, 1, 'Review code')");

        $stmt = $pdo->query("SELECT name FROM emp e WHERE EXISTS (SELECT 1 FROM tasks t WHERE t.emp_id = e.id)");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testNotExistsSubquery(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT)');
        $raw->exec('CREATE TABLE tasks (id INTEGER PRIMARY KEY, emp_id INTEGER, task TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO emp (id, name) VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO emp (id, name) VALUES (2, 'Bob')");
        $pdo->exec("INSERT INTO tasks (id, emp_id, task) VALUES (1, 1, 'Review code')");

        $stmt = $pdo->query("SELECT name FROM emp e WHERE NOT EXISTS (SELECT 1 FROM tasks t WHERE t.emp_id = e.id)");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testCoalesce(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE contact (id INTEGER PRIMARY KEY, name TEXT, email TEXT, phone TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO contact (id, name, email, phone) VALUES (1, 'Alice', 'alice@example.com', NULL)");
        $pdo->exec("INSERT INTO contact (id, name, email, phone) VALUES (2, 'Bob', NULL, '555-0100')");

        $stmt = $pdo->query("SELECT name, COALESCE(email, phone, 'N/A') AS contact_info FROM contact ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('alice@example.com', $rows[0]['contact_info']);
        $this->assertSame('555-0100', $rows[1]['contact_info']);
    }

    public function testWindowFunctionRowNumber(): void
    {
        $stmt = $this->pdo->query("
            SELECT name, department, salary,
                ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
            FROM employees ORDER BY department, rank_in_dept
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(5, $rows);

        // Engineering: Charlie (110k) rank 1, Alice (90k) rank 2
        $engineering = array_filter($rows, fn($r) => $r['department'] === 'Engineering');
        $engineering = array_values($engineering);
        $this->assertSame('Charlie', $engineering[0]['name']);
        $this->assertSame(1, (int) $engineering[0]['rank_in_dept']);
        $this->assertSame('Alice', $engineering[1]['name']);
        $this->assertSame(2, (int) $engineering[1]['rank_in_dept']);
    }

    public function testWindowFunctionSum(): void
    {
        $stmt = $this->pdo->query("
            SELECT name, department, salary,
                SUM(salary) OVER (PARTITION BY department) AS dept_total
            FROM employees WHERE department = 'Engineering' ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        // Both should show the same department total
        $this->assertSame(200000.0, (float) $rows[0]['dept_total']);
        $this->assertSame(200000.0, (float) $rows[1]['dept_total']);
    }

    public function testUpdateWithCaseExpression(): void
    {
        $this->pdo->exec("
            UPDATE employees SET salary = CASE
                WHEN department = 'Engineering' THEN salary * 1.1
                ELSE salary * 1.05
            END
        ");

        $stmt = $this->pdo->query("SELECT name, salary FROM employees ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(99000.0, (float) $rows[0]['salary']);  // Alice: 90000 * 1.1
        $this->assertSame(63000.0, (float) $rows[1]['salary']);  // Bob: 60000 * 1.05
        $this->assertSame(121000.0, (float) $rows[2]['salary']); // Charlie: 110000 * 1.1
    }
}
