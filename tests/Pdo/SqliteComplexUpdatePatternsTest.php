<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests complex UPDATE patterns: CASE in SET, arithmetic expressions,
 * string concatenation in SET, UPDATE with subquery in SET,
 * multiple sequential mutations, and prepared UPDATE with CASE.
 */
class SqliteComplexUpdatePatternsTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL, grade TEXT, active INTEGER)');
        $raw->exec('CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL, min_salary REAL)');

        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 80000, 'B', 1)");
        $this->pdo->exec("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 90000, 'A', 1)");
        $this->pdo->exec("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 60000, 'C', 1)");
        $this->pdo->exec("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 70000, 'B', 0)");
        $this->pdo->exec("INSERT INTO employees VALUES (5, 'Eve', 'Marketing', 55000, 'C', 1)");

        $this->pdo->exec("INSERT INTO departments VALUES (1, 'Engineering', 500000, 75000)");
        $this->pdo->exec("INSERT INTO departments VALUES (2, 'Sales', 300000, 55000)");
        $this->pdo->exec("INSERT INTO departments VALUES (3, 'Marketing', 200000, 50000)");
    }

    public function testUpdateWithCaseInSet(): void
    {
        $this->pdo->exec("
            UPDATE employees SET grade = CASE
                WHEN salary >= 90000 THEN 'S'
                WHEN salary >= 75000 THEN 'A'
                WHEN salary >= 60000 THEN 'B'
                ELSE 'C'
            END
        ");

        $stmt = $this->pdo->query("SELECT name, grade FROM employees ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['grade']); // Alice: 80000
        $this->assertSame('S', $rows[1]['grade']); // Bob: 90000
        $this->assertSame('B', $rows[2]['grade']); // Charlie: 60000
        $this->assertSame('B', $rows[3]['grade']); // Diana: 70000
        $this->assertSame('C', $rows[4]['grade']); // Eve: 55000
    }

    public function testUpdateWithArithmeticPercentIncrease(): void
    {
        // 10% raise for Engineering
        $this->pdo->exec("UPDATE employees SET salary = salary * 1.10 WHERE department = 'Engineering'");

        $stmt = $this->pdo->query("SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(88000.0, (float) $rows[0]['salary'], 0.01); // Alice
        $this->assertEqualsWithDelta(99000.0, (float) $rows[1]['salary'], 0.01); // Bob
    }

    public function testUpdateWithStringConcatenation(): void
    {
        $this->pdo->exec("UPDATE employees SET name = name || ' (inactive)' WHERE active = 0");

        $stmt = $this->pdo->query("SELECT name FROM employees WHERE id = 4");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Diana (inactive)', $row['name']);
    }

    public function testMultipleSequentialUpdates(): void
    {
        // Step 1: Give everyone a 5% raise
        $this->pdo->exec("UPDATE employees SET salary = salary * 1.05");

        // Step 2: Deactivate low performers
        $this->pdo->exec("UPDATE employees SET active = 0 WHERE grade = 'C'");

        // Step 3: Promote active high earners
        $this->pdo->exec("UPDATE employees SET grade = 'S' WHERE salary > 80000 AND active = 1");

        // Verify all three mutations applied correctly
        $stmt = $this->pdo->query("SELECT name, salary, grade, active FROM employees ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Alice: 80000*1.05=84000, grade B, active 1 -> grade S (84000>80000)
        $this->assertEqualsWithDelta(84000.0, (float) $rows[0]['salary'], 0.01);
        $this->assertSame('S', $rows[0]['grade']);

        // Bob: 90000*1.05=94500, grade A, active 1 -> grade S (94500>80000)
        $this->assertEqualsWithDelta(94500.0, (float) $rows[1]['salary'], 0.01);
        $this->assertSame('S', $rows[1]['grade']);

        // Charlie: 60000*1.05=63000, grade C -> active 0 -> grade still C (active=0)
        $this->assertSame(0, (int) $rows[2]['active']);
        $this->assertSame('C', $rows[2]['grade']);

        // Eve: 55000*1.05=57750, grade C -> active 0
        $this->assertSame(0, (int) $rows[4]['active']);
    }

    public function testUpdateWithConditionalArithmetic(): void
    {
        // Different raise percentages by grade
        $this->pdo->exec("
            UPDATE employees SET salary = CASE
                WHEN grade = 'A' THEN salary * 1.15
                WHEN grade = 'B' THEN salary * 1.10
                WHEN grade = 'C' THEN salary * 1.05
                ELSE salary
            END
            WHERE active = 1
        ");

        $stmt = $this->pdo->query("SELECT name, salary FROM employees WHERE active = 1 ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(88000.0, (float) $rows[0]['salary'], 0.01); // Alice B: +10%
        $this->assertEqualsWithDelta(103500.0, (float) $rows[1]['salary'], 0.01); // Bob A: +15%
        $this->assertEqualsWithDelta(63000.0, (float) $rows[2]['salary'], 0.01); // Charlie C: +5%
        $this->assertEqualsWithDelta(57750.0, (float) $rows[3]['salary'], 0.01); // Eve C: +5%
    }

    public function testDeleteThenInsertSameId(): void
    {
        $this->pdo->exec("DELETE FROM employees WHERE id = 3");
        $this->pdo->exec("INSERT INTO employees VALUES (3, 'Frank', 'Engineering', 95000, 'S', 1)");

        $stmt = $this->pdo->query("SELECT name, department FROM employees WHERE id = 3");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Frank', $row['name']);
        $this->assertSame('Engineering', $row['department']);
    }

    public function testUpdateThenDeleteThenCount(): void
    {
        $this->pdo->exec("UPDATE employees SET active = 0 WHERE department = 'Sales'");
        $this->pdo->exec("DELETE FROM employees WHERE active = 0");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM employees");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']); // Alice, Bob, Eve remain
    }

    public function testInsertThenUpdateThenQuery(): void
    {
        $this->pdo->exec("INSERT INTO employees VALUES (6, 'George', 'Engineering', 100000, 'A', 1)");
        $this->pdo->exec("UPDATE employees SET grade = 'S' WHERE salary >= 100000");

        $stmt = $this->pdo->query("SELECT name, grade FROM employees WHERE grade = 'S' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('George', $rows[0]['name']);
    }

    public function testPreparedUpdateWithCase(): void
    {
        $stmt = $this->pdo->prepare("
            UPDATE employees SET grade = CASE
                WHEN salary >= ? THEN 'S'
                WHEN salary >= ? THEN 'A'
                ELSE grade
            END
            WHERE department = ?
        ");
        $stmt->execute([85000, 75000, 'Engineering']);

        $select = $this->pdo->query("SELECT name, grade FROM employees WHERE department = 'Engineering' ORDER BY id");
        $rows = $select->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['grade']); // Alice: 80000 >= 75000
        $this->assertSame('S', $rows[1]['grade']); // Bob: 90000 >= 85000
    }

    public function testShadowIsolationAfterComplexMutations(): void
    {
        $this->pdo->exec("UPDATE employees SET salary = salary * 2");
        $this->pdo->exec("DELETE FROM employees WHERE id > 3");
        $this->pdo->exec("INSERT INTO employees VALUES (6, 'New', 'HR', 50000, 'C', 1)");

        // Shadow data reflects all mutations
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM employees");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['cnt']); // 3 original (id<=3) + 1 new

        // Physical DB untouched
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM employees");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }
}
