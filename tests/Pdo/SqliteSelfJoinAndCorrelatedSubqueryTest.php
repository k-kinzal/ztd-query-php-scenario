<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests self-joins and correlated subqueries with ZTD shadow store on SQLite.
 * These patterns require the CTE rewriter to handle multiple references to
 * the same table correctly.
 */
class SqliteSelfJoinAndCorrelatedSubqueryTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(50), manager_id INT, salary INT, dept VARCHAR(20))');
        $this->pdo->exec("INSERT INTO employees VALUES (1, 'CEO', NULL, 200, 'exec')");
        $this->pdo->exec("INSERT INTO employees VALUES (2, 'VP', 1, 150, 'exec')");
        $this->pdo->exec("INSERT INTO employees VALUES (3, 'Alice', 2, 100, 'eng')");
        $this->pdo->exec("INSERT INTO employees VALUES (4, 'Bob', 2, 90, 'eng')");
        $this->pdo->exec("INSERT INTO employees VALUES (5, 'Charlie', 3, 80, 'eng')");
    }

    public function testSelfJoinEmployeeManager(): void
    {
        $stmt = $this->pdo->query(
            'SELECT e.name AS employee, m.name AS manager
             FROM employees e
             LEFT JOIN employees m ON e.manager_id = m.id
             ORDER BY e.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
        $this->assertSame('VP', $rows[1]['employee']);
        $this->assertSame('CEO', $rows[1]['manager']);
        $this->assertSame('Alice', $rows[2]['employee']);
        $this->assertSame('VP', $rows[2]['manager']);
    }

    public function testSelfJoinWithPreparedParam(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT e.name AS employee, m.name AS manager
             FROM employees e
             LEFT JOIN employees m ON e.manager_id = m.id
             WHERE e.dept = ?
             ORDER BY e.name'
        );
        $stmt->execute(['eng']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['employee']);
        $this->assertSame('VP', $rows[0]['manager']);
    }

    public function testCorrelatedSubqueryAboveAverage(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name, salary FROM employees e
             WHERE salary > (SELECT AVG(salary) FROM employees)
             ORDER BY salary DESC'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Average salary = (200+150+100+90+80)/5 = 124
        $this->assertCount(2, $rows);
        $this->assertSame('CEO', $rows[0]['name']);
        $this->assertSame('VP', $rows[1]['name']);
    }

    public function testCorrelatedSubqueryWithDeptFilter(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name, salary FROM employees e
             WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept = e.dept)
             ORDER BY name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // CEO (200) > exec avg (175) = yes; VP (150) < exec avg (175) = no
        // Alice (100) > eng avg (90) = yes; Bob (90) = eng avg = no; Charlie (80) < eng avg = no
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('CEO', $rows[1]['name']);
    }

    public function testExistsSubquery(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name FROM employees e
             WHERE EXISTS (SELECT 1 FROM employees sub WHERE sub.manager_id = e.id)
             ORDER BY name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // CEO has VP as report, VP has Alice+Bob, Alice has Charlie
        $this->assertCount(3, $rows);
    }

    public function testNotExistsSubquery(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name FROM employees e
             WHERE NOT EXISTS (SELECT 1 FROM employees sub WHERE sub.manager_id = e.id)
             ORDER BY name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Bob and Charlie have no reports
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testScalarSubqueryInSelect(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name,
                    (SELECT COUNT(*) FROM employees sub WHERE sub.manager_id = e.id) AS report_count
             FROM employees e
             ORDER BY name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $byName = array_column($rows, 'report_count', 'name');
        $this->assertSame(1, (int) $byName['CEO']);   // VP
        $this->assertSame(2, (int) $byName['VP']);    // Alice, Bob
        $this->assertSame(1, (int) $byName['Alice']); // Charlie
        $this->assertSame(0, (int) $byName['Bob']);
        $this->assertSame(0, (int) $byName['Charlie']);
    }

    public function testSelfJoinAfterMutation(): void
    {
        // Add a new employee
        $this->pdo->exec("INSERT INTO employees VALUES (6, 'Diana', 3, 85, 'eng')");

        $stmt = $this->pdo->query(
            'SELECT e.name AS employee, m.name AS manager
             FROM employees e
             LEFT JOIN employees m ON e.manager_id = m.id
             WHERE e.name = \'Diana\''
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Diana', $row['employee']);
        $this->assertSame('Alice', $row['manager']);
    }

    public function testDeleteWithSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM employees WHERE id IN (SELECT e.id FROM employees e WHERE e.salary < 85)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM employees');
        $cnt = (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt'];
        // Charlie (80) removed, 4 remain
        $this->assertSame(4, $cnt);
    }

    public function testUpdateWithSubquery(): void
    {
        $this->pdo->exec(
            "UPDATE employees SET salary = salary + 10 WHERE id IN (SELECT e.id FROM employees e WHERE e.dept = 'eng')"
        );

        $stmt = $this->pdo->query("SELECT salary FROM employees WHERE name = 'Alice'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(110, (int) $row['salary']);
    }
}
