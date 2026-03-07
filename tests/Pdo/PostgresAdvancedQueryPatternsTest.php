<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests advanced SQL query patterns in ZTD mode on PostgreSQL:
 * CASE, LIKE, IN, BETWEEN, EXISTS, COALESCE, window functions.
 */
class PostgresAdvancedQueryPatternsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_adv_employees');
        $raw->exec('CREATE TABLE pg_adv_employees (id INT PRIMARY KEY, name VARCHAR(255), department VARCHAR(255), salary NUMERIC(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_adv_employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->pdo->exec("INSERT INTO pg_adv_employees (id, name, department, salary) VALUES (2, 'Bob', 'Sales', 60000)");
        $this->pdo->exec("INSERT INTO pg_adv_employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 110000)");
        $this->pdo->exec("INSERT INTO pg_adv_employees (id, name, department, salary) VALUES (4, 'Diana', 'Marketing', 75000)");
        $this->pdo->exec("INSERT INTO pg_adv_employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 55000)");
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
            FROM pg_adv_employees ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('medium', $rows[0]['salary_band']); // Alice 90k
        $this->assertSame('low', $rows[1]['salary_band']);     // Bob 60k
        $this->assertSame('high', $rows[2]['salary_band']);    // Charlie 110k
    }

    public function testLikeClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_adv_employees WHERE name LIKE 'A%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testLikeClauseWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_adv_employees WHERE name LIKE '%li%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows); // Alice, Charlie
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testInClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_adv_employees WHERE department IN ('Engineering', 'Marketing') ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
        $this->assertSame('Diana', $rows[2]['name']);
    }

    public function testBetweenClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_adv_employees WHERE salary BETWEEN 60000 AND 80000 ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
    }

    public function testExistsSubquery(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_adv_tasks');
        $raw->exec('DROP TABLE IF EXISTS pg_adv_emp');
        $raw->exec('CREATE TABLE pg_adv_emp (id INT PRIMARY KEY, name VARCHAR(255))');
        $raw->exec('CREATE TABLE pg_adv_tasks (id INT PRIMARY KEY, emp_id INT, task VARCHAR(255))');

        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $pdo->exec("INSERT INTO pg_adv_emp (id, name) VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO pg_adv_emp (id, name) VALUES (2, 'Bob')");
        $pdo->exec("INSERT INTO pg_adv_tasks (id, emp_id, task) VALUES (1, 1, 'Review code')");

        $stmt = $pdo->query("SELECT name FROM pg_adv_emp e WHERE EXISTS (SELECT 1 FROM pg_adv_tasks t WHERE t.emp_id = e.id)");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $raw->exec('DROP TABLE IF EXISTS pg_adv_tasks');
        $raw->exec('DROP TABLE IF EXISTS pg_adv_emp');
    }

    public function testNotExistsSubquery(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_adv_tasks2');
        $raw->exec('DROP TABLE IF EXISTS pg_adv_emp2');
        $raw->exec('CREATE TABLE pg_adv_emp2 (id INT PRIMARY KEY, name VARCHAR(255))');
        $raw->exec('CREATE TABLE pg_adv_tasks2 (id INT PRIMARY KEY, emp_id INT, task VARCHAR(255))');

        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $pdo->exec("INSERT INTO pg_adv_emp2 (id, name) VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO pg_adv_emp2 (id, name) VALUES (2, 'Bob')");
        $pdo->exec("INSERT INTO pg_adv_tasks2 (id, emp_id, task) VALUES (1, 1, 'Review code')");

        $stmt = $pdo->query("SELECT name FROM pg_adv_emp2 e WHERE NOT EXISTS (SELECT 1 FROM pg_adv_tasks2 t WHERE t.emp_id = e.id)");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);

        $raw->exec('DROP TABLE IF EXISTS pg_adv_tasks2');
        $raw->exec('DROP TABLE IF EXISTS pg_adv_emp2');
    }

    public function testCoalesce(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_adv_contact');
        $raw->exec('CREATE TABLE pg_adv_contact (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), phone VARCHAR(255))');

        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $pdo->exec("INSERT INTO pg_adv_contact (id, name, email, phone) VALUES (1, 'Alice', 'alice@example.com', NULL)");
        $pdo->exec("INSERT INTO pg_adv_contact (id, name, email, phone) VALUES (2, 'Bob', NULL, '555-0100')");

        $stmt = $pdo->query("SELECT name, COALESCE(email, phone, 'N/A') AS contact_info FROM pg_adv_contact ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('alice@example.com', $rows[0]['contact_info']);
        $this->assertSame('555-0100', $rows[1]['contact_info']);

        $raw->exec('DROP TABLE IF EXISTS pg_adv_contact');
    }

    public function testWindowFunctionRowNumber(): void
    {
        $stmt = $this->pdo->query("
            SELECT name, department, salary,
                ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
            FROM pg_adv_employees ORDER BY department, rank_in_dept
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

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
        $stmt = $this->pdo->query("
            SELECT name, department, salary,
                SUM(salary) OVER (PARTITION BY department) AS dept_total
            FROM pg_adv_employees WHERE department = 'Engineering' ORDER BY name
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
            UPDATE pg_adv_employees SET salary = CASE
                WHEN department = 'Engineering' THEN salary * 1.1
                ELSE salary * 1.05
            END
        ");

        $stmt = $this->pdo->query("SELECT name, salary FROM pg_adv_employees ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(99000.0, (float) $rows[0]['salary']);  // Alice: 90000 * 1.1
        $this->assertSame(63000.0, (float) $rows[1]['salary']);  // Bob: 60000 * 1.05
        $this->assertSame(121000.0, (float) $rows[2]['salary']); // Charlie: 110000 * 1.1
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_adv_employees');
    }
}
