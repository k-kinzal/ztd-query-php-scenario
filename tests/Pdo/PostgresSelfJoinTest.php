<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests self-JOIN patterns through ZTD shadow store on PostgreSQL.
 *
 * Self-JOINs reference the same table twice with different aliases.
 * The CTE rewriter must correctly rewrite both table references
 * to their shadow CTE equivalents.
 * @spec SPEC-3.3
 */
class PostgresSelfJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sj_employees (id INT PRIMARY KEY, name VARCHAR(50), manager_id INT, salary INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sj_employees VALUES (1, 'CEO', NULL, 200000)");
        $this->pdo->exec("INSERT INTO sj_employees VALUES (2, 'VP', 1, 150000)");
        $this->pdo->exec("INSERT INTO sj_employees VALUES (3, 'Manager', 2, 100000)");
        $this->pdo->exec("INSERT INTO sj_employees VALUES (4, 'Dev', 3, 80000)");
        $this->pdo->exec("INSERT INTO sj_employees VALUES (5, 'Intern', 3, 40000)");
    }

    /**
     * Basic self-JOIN: employee with manager name.
     */
    public function testSelfJoinEmployeeManager(): void
    {
        $rows = $this->ztdQuery(
            'SELECT e.name AS employee, m.name AS manager
             FROM sj_employees e
             LEFT JOIN sj_employees m ON e.manager_id = m.id
             ORDER BY e.id'
        );

        $this->assertCount(5, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
        $this->assertSame('VP', $rows[1]['employee']);
        $this->assertSame('CEO', $rows[1]['manager']);
        $this->assertSame('Dev', $rows[3]['employee']);
        $this->assertSame('Manager', $rows[3]['manager']);
    }

    /**
     * Self-JOIN with INNER JOIN (only employees who have managers).
     */
    public function testSelfJoinInnerOnly(): void
    {
        $rows = $this->ztdQuery(
            'SELECT e.name AS employee, m.name AS manager
             FROM sj_employees e
             INNER JOIN sj_employees m ON e.manager_id = m.id
             ORDER BY e.id'
        );

        // CEO has no manager, so only 4 rows
        $this->assertCount(4, $rows);
        $this->assertSame('VP', $rows[0]['employee']);
    }

    /**
     * Self-JOIN after INSERT mutation: new employee should appear in join.
     */
    public function testSelfJoinAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sj_employees VALUES (6, 'Junior', 4, 50000)");

        $rows = $this->ztdQuery(
            'SELECT e.name AS employee, m.name AS manager
             FROM sj_employees e
             LEFT JOIN sj_employees m ON e.manager_id = m.id
             WHERE e.id = 6'
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Junior', $rows[0]['employee']);
        $this->assertSame('Dev', $rows[0]['manager']);
    }

    /**
     * Self-JOIN after UPDATE mutation: changed manager should reflect in join.
     */
    public function testSelfJoinAfterUpdate(): void
    {
        // Reassign Dev to report to VP instead of Manager
        $this->pdo->exec('UPDATE sj_employees SET manager_id = 2 WHERE id = 4');

        $rows = $this->ztdQuery(
            'SELECT e.name AS employee, m.name AS manager
             FROM sj_employees e
             LEFT JOIN sj_employees m ON e.manager_id = m.id
             WHERE e.id = 4'
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Dev', $rows[0]['employee']);
        $this->assertSame('VP', $rows[0]['manager']);
    }

    /**
     * Self-JOIN after DELETE: removed manager should show NULL.
     */
    public function testSelfJoinAfterDelete(): void
    {
        $this->pdo->exec('DELETE FROM sj_employees WHERE id = 3');

        $rows = $this->ztdQuery(
            'SELECT e.name AS employee, m.name AS manager
             FROM sj_employees e
             LEFT JOIN sj_employees m ON e.manager_id = m.id
             WHERE e.id IN (4, 5)
             ORDER BY e.id'
        );

        $this->assertCount(2, $rows);
        // Manager was deleted, so these employees have no match
        $this->assertNull($rows[0]['manager']);
        $this->assertNull($rows[1]['manager']);
    }

    /**
     * Self-JOIN with aggregate: count of direct reports per manager.
     */
    public function testSelfJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            'SELECT m.name AS manager, COUNT(e.id) AS report_count
             FROM sj_employees m
             LEFT JOIN sj_employees e ON e.manager_id = m.id
             GROUP BY m.id, m.name
             HAVING COUNT(e.id) > 0
             ORDER BY report_count DESC'
        );

        // CEO->1 report (VP), VP->1 (Manager), Manager->2 (Dev, Intern)
        $this->assertCount(3, $rows);
        $this->assertSame('Manager', $rows[0]['manager']);
        $this->assertSame('2', (string) $rows[0]['report_count']);
    }

    /**
     * Triple self-JOIN: employee -> manager -> grand-manager.
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            'SELECT e.name AS employee, m.name AS manager, gm.name AS grand_manager
             FROM sj_employees e
             LEFT JOIN sj_employees m ON e.manager_id = m.id
             LEFT JOIN sj_employees gm ON m.manager_id = gm.id
             ORDER BY e.id'
        );

        $this->assertCount(5, $rows);
        // Dev's manager is Manager, Manager's manager is VP
        $this->assertSame('Dev', $rows[3]['employee']);
        $this->assertSame('Manager', $rows[3]['manager']);
        $this->assertSame('VP', $rows[3]['grand_manager']);
    }

    /**
     * Self-JOIN with WHERE on both aliases.
     */
    public function testSelfJoinFilterBothSides(): void
    {
        $rows = $this->ztdQuery(
            'SELECT e.name AS employee, m.name AS manager
             FROM sj_employees e
             JOIN sj_employees m ON e.manager_id = m.id
             WHERE e.salary < 100000 AND m.salary >= 100000
             ORDER BY e.name'
        );

        // Dev (80k) -> Manager (100k), Intern (40k) -> Manager (100k)
        $this->assertCount(2, $rows);
        $this->assertSame('Dev', $rows[0]['employee']);
        $this->assertSame('Manager', $rows[0]['manager']);
        $this->assertSame('Intern', $rows[1]['employee']);
        $this->assertSame('Manager', $rows[1]['manager']);
    }

    /**
     * Physical table isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sj_employees');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
