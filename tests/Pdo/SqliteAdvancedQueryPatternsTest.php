<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests advanced SQL query patterns in ZTD mode on SQLite:
 * CASE, LIKE, IN, BETWEEN, EXISTS, COALESCE, window functions.
 * @spec SPEC-3.3
 */
class SqliteAdvancedQueryPatternsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL)',
            'CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE tasks (id INTEGER PRIMARY KEY, emp_id INTEGER, task TEXT)',
            'CREATE TABLE emp2 (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE tasks2 (id INTEGER PRIMARY KEY, emp_id INTEGER, task TEXT)',
            'CREATE TABLE contact (id INTEGER PRIMARY KEY, name TEXT, email TEXT, phone TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['employees', 'emp', 'tasks', 'emp2', 'tasks2', 'contact'];
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
        $raw->exec('CREATE TABLE emp2 (id INTEGER PRIMARY KEY, name TEXT)');
        $raw->exec('CREATE TABLE tasks2 (id INTEGER PRIMARY KEY, emp_id INTEGER, task TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO emp2 (id, name) VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO emp2 (id, name) VALUES (2, 'Bob')");
        $pdo->exec("INSERT INTO tasks2 (id, emp_id, task) VALUES (1, 1, 'Review code')");

        $stmt = $pdo->query("SELECT name FROM emp2 e WHERE NOT EXISTS (SELECT 1 FROM tasks2 t WHERE t.emp_id = e.id)");
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

        $engineering = array_values(array_filter($rows, fn($r) => $r['department'] === 'Engineering'));
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
