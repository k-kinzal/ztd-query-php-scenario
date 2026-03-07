<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests complex UPDATE patterns on MySQL PDO: CASE in SET, arithmetic expressions,
 * multiple sequential mutations, prepared UPDATE with CASE.
 */
class MysqlComplexUpdatePatternsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_cup_employees');
        $raw->exec('CREATE TABLE mysql_cup_employees (id INT PRIMARY KEY, name VARCHAR(50), department VARCHAR(30), salary DECIMAL(10,2), grade VARCHAR(5), active TINYINT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO mysql_cup_employees VALUES (1, 'Alice', 'Engineering', 80000, 'B', 1)");
        $this->pdo->exec("INSERT INTO mysql_cup_employees VALUES (2, 'Bob', 'Engineering', 90000, 'A', 1)");
        $this->pdo->exec("INSERT INTO mysql_cup_employees VALUES (3, 'Charlie', 'Sales', 60000, 'C', 1)");
        $this->pdo->exec("INSERT INTO mysql_cup_employees VALUES (4, 'Diana', 'Sales', 70000, 'B', 0)");
        $this->pdo->exec("INSERT INTO mysql_cup_employees VALUES (5, 'Eve', 'Marketing', 55000, 'C', 1)");
    }

    public function testUpdateWithCaseInSet(): void
    {
        $this->pdo->exec("
            UPDATE mysql_cup_employees SET grade = CASE
                WHEN salary >= 90000 THEN 'S'
                WHEN salary >= 75000 THEN 'A'
                WHEN salary >= 60000 THEN 'B'
                ELSE 'C'
            END
        ");

        $stmt = $this->pdo->query("SELECT name, grade FROM mysql_cup_employees ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['grade']);
        $this->assertSame('S', $rows[1]['grade']);
        $this->assertSame('B', $rows[2]['grade']);
    }

    public function testUpdateWithConditionalArithmetic(): void
    {
        $this->pdo->exec("
            UPDATE mysql_cup_employees SET salary = CASE
                WHEN grade = 'A' THEN salary * 1.15
                WHEN grade = 'B' THEN salary * 1.10
                ELSE salary * 1.05
            END
            WHERE active = 1
        ");

        $stmt = $this->pdo->query("SELECT name, salary FROM mysql_cup_employees WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(88000.0, (float) $row['salary'], 0.01);
    }

    public function testMultipleSequentialUpdates(): void
    {
        $this->pdo->exec("UPDATE mysql_cup_employees SET salary = salary * 1.05");
        $this->pdo->exec("UPDATE mysql_cup_employees SET active = 0 WHERE grade = 'C'");
        $this->pdo->exec("UPDATE mysql_cup_employees SET grade = 'S' WHERE salary > 80000 AND active = 1");

        $stmt = $this->pdo->query("SELECT name, grade, active FROM mysql_cup_employees WHERE id = 2");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('S', $row['grade']); // Bob: 94500 > 80000, active
    }

    public function testUpdateThenDeleteThenCount(): void
    {
        $this->pdo->exec("UPDATE mysql_cup_employees SET active = 0 WHERE department = 'Sales'");
        $this->pdo->exec("DELETE FROM mysql_cup_employees WHERE active = 0");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_cup_employees");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testPreparedUpdateWithCase(): void
    {
        $stmt = $this->pdo->prepare("
            UPDATE mysql_cup_employees SET grade = CASE
                WHEN salary >= ? THEN 'S'
                WHEN salary >= ? THEN 'A'
                ELSE grade
            END
            WHERE department = ?
        ");
        $stmt->execute([85000, 75000, 'Engineering']);

        $select = $this->pdo->query("SELECT name, grade FROM mysql_cup_employees WHERE department = 'Engineering' ORDER BY id");
        $rows = $select->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['grade']);
        $this->assertSame('S', $rows[1]['grade']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_cup_employees');
    }
}
