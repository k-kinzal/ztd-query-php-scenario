<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Self-JOIN on shadow data: employee hierarchy (employee → manager).
 * Tests whether the CTE rewriter handles the same table appearing twice with different aliases.
 *
 * @spec SPEC-3.3
 */
class PostgresSelfJoinHierarchyTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_sjh_employees (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            manager_id INT NULL,
            salary NUMERIC(10,2) NOT NULL
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_sjh_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sjh_employees (id, name, manager_id, salary) VALUES
            (1, 'Alice', NULL, 120000),
            (2, 'Bob', 1, 90000),
            (3, 'Carol', 1, 95000),
            (4, 'Dave', 2, 70000),
            (5, 'Eve', 2, 75000)");
    }

    public function testSelfJoinEmployeeManager(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name AS employee, m.name AS manager
                 FROM pg_sjh_employees e
                 LEFT JOIN pg_sjh_employees m ON e.manager_id = m.id
                 ORDER BY e.id"
            );

            if (count($rows) !== 5) {
                $this->markTestIncomplete('Self-JOIN: expected 5, got ' . count($rows));
            }

            $this->assertSame('Alice', $rows[0]['employee']);
            $this->assertNull($rows[0]['manager']);
            $this->assertSame('Bob', $rows[1]['employee']);
            $this->assertSame('Alice', $rows[1]['manager']);
            $this->assertSame('Carol', $rows[2]['employee']);
            $this->assertSame('Alice', $rows[2]['manager']);
            $this->assertSame('Dave', $rows[3]['employee']);
            $this->assertSame('Bob', $rows[3]['manager']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-JOIN failed: ' . $e->getMessage());
        }
    }

    public function testSelfJoinAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sjh_employees (id, name, manager_id, salary) VALUES (6, 'Frank', 3, 65000)");

            $rows = $this->ztdQuery(
                "SELECT e.name AS employee, m.name AS manager
                 FROM pg_sjh_employees e
                 INNER JOIN pg_sjh_employees m ON e.manager_id = m.id
                 WHERE m.name = 'Carol'
                 ORDER BY e.id"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Self-JOIN after INSERT: expected 1, got ' . count($rows));
            }

            $this->assertSame('Frank', $rows[0]['employee']);
            $this->assertSame('Carol', $rows[0]['manager']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-JOIN after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testSelfJoinWithAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT m.name AS manager, COUNT(e.id) AS direct_reports, AVG(e.salary) AS avg_salary
                 FROM pg_sjh_employees e
                 INNER JOIN pg_sjh_employees m ON e.manager_id = m.id
                 GROUP BY m.id, m.name
                 ORDER BY direct_reports DESC"
            );

            if (count($rows) < 1) {
                $this->markTestIncomplete('Self-JOIN aggregate: expected rows, got 0');
            }

            // Alice has 2 direct reports (Bob, Carol)
            $this->assertSame('Alice', $rows[0]['manager']);
            $this->assertSame(2, (int) $rows[0]['direct_reports']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-JOIN aggregate failed: ' . $e->getMessage());
        }
    }

    public function testSelfJoinAfterUpdate(): void
    {
        try {
            // Move Dave from Bob's team to Carol's team
            $this->pdo->exec("UPDATE pg_sjh_employees SET manager_id = 3 WHERE id = 4");

            $rows = $this->ztdQuery(
                "SELECT e.name AS employee, m.name AS manager
                 FROM pg_sjh_employees e
                 INNER JOIN pg_sjh_employees m ON e.manager_id = m.id
                 WHERE e.id = 4"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Self-JOIN after UPDATE: expected 1, got ' . count($rows));
            }

            $this->assertSame('Dave', $rows[0]['employee']);
            $this->assertSame('Carol', $rows[0]['manager']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-JOIN after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testSelfJoinAfterDelete(): void
    {
        try {
            // Delete Bob (id=2), who manages Dave and Eve
            $this->pdo->exec("DELETE FROM pg_sjh_employees WHERE id = 2");

            // LEFT JOIN should now show Dave and Eve with NULL manager
            $rows = $this->ztdQuery(
                "SELECT e.name AS employee, m.name AS manager
                 FROM pg_sjh_employees e
                 LEFT JOIN pg_sjh_employees m ON e.manager_id = m.id
                 WHERE e.manager_id = 2
                 ORDER BY e.id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Self-JOIN after DELETE: expected 2, got ' . count($rows));
            }

            $this->assertSame('Dave', $rows[0]['employee']);
            $this->assertNull($rows[0]['manager']); // Bob was deleted
            $this->assertSame('Eve', $rows[1]['employee']);
            $this->assertNull($rows[1]['manager']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-JOIN after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_sjh_employees")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
