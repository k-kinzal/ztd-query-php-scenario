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
 * Tests complex UPDATE patterns on PostgreSQL PDO: CASE in SET, arithmetic expressions,
 * multiple sequential mutations, string concatenation, prepared UPDATE with CASE.
 */
class PostgresComplexUpdatePatternsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_cup_employees');
        $raw->exec('CREATE TABLE pg_cup_employees (id INT PRIMARY KEY, name VARCHAR(50), department VARCHAR(30), salary NUMERIC(10,2), grade VARCHAR(5), active SMALLINT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_cup_employees VALUES (1, 'Alice', 'Engineering', 80000, 'B', 1)");
        $this->pdo->exec("INSERT INTO pg_cup_employees VALUES (2, 'Bob', 'Engineering', 90000, 'A', 1)");
        $this->pdo->exec("INSERT INTO pg_cup_employees VALUES (3, 'Charlie', 'Sales', 60000, 'C', 1)");
        $this->pdo->exec("INSERT INTO pg_cup_employees VALUES (4, 'Diana', 'Sales', 70000, 'B', 0)");
        $this->pdo->exec("INSERT INTO pg_cup_employees VALUES (5, 'Eve', 'Marketing', 55000, 'C', 1)");
    }

    public function testUpdateWithCaseInSet(): void
    {
        $this->pdo->exec("
            UPDATE pg_cup_employees SET grade = CASE
                WHEN salary >= 90000 THEN 'S'
                WHEN salary >= 75000 THEN 'A'
                WHEN salary >= 60000 THEN 'B'
                ELSE 'C'
            END
        ");

        $stmt = $this->pdo->query("SELECT name, grade FROM pg_cup_employees ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['grade']);
        $this->assertSame('S', $rows[1]['grade']);
        $this->assertSame('B', $rows[2]['grade']);
    }

    public function testUpdateWithConditionalArithmetic(): void
    {
        $this->pdo->exec("
            UPDATE pg_cup_employees SET salary = CASE
                WHEN grade = 'A' THEN salary * 1.15
                WHEN grade = 'B' THEN salary * 1.10
                ELSE salary * 1.05
            END
            WHERE active = 1
        ");

        $stmt = $this->pdo->query("SELECT name, salary FROM pg_cup_employees WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(88000.0, (float) $row['salary'], 0.01);
    }

    public function testUpdateWithStringConcatenation(): void
    {
        $this->pdo->exec("UPDATE pg_cup_employees SET name = name || ' (inactive)' WHERE active = 0");

        $stmt = $this->pdo->query("SELECT name FROM pg_cup_employees WHERE id = 4");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Diana (inactive)', $row['name']);
    }

    public function testMultipleSequentialUpdates(): void
    {
        $this->pdo->exec("UPDATE pg_cup_employees SET salary = salary * 1.05");
        $this->pdo->exec("UPDATE pg_cup_employees SET active = 0 WHERE grade = 'C'");
        $this->pdo->exec("UPDATE pg_cup_employees SET grade = 'S' WHERE salary > 80000 AND active = 1");

        $stmt = $this->pdo->query("SELECT name, grade, active FROM pg_cup_employees WHERE id = 2");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('S', $row['grade']);
    }

    public function testUpdateThenDeleteThenCount(): void
    {
        $this->pdo->exec("UPDATE pg_cup_employees SET active = 0 WHERE department = 'Sales'");
        $this->pdo->exec("DELETE FROM pg_cup_employees WHERE active = 0");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_cup_employees");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_cup_employees');
    }
}
