<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests nested CASE expressions through ZTD shadow store on SQLite:
 * - Nested CASE in SELECT
 * - Nested CASE in ORDER BY
 * - Nested CASE in UPDATE SET clause
 * - Multiple WHEN branches with nested inner CASE
 * - Prepared statement with nested CASE and bound parameters
 *
 * @spec SPEC-3.3
 */
class SqliteNestedCaseExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, role TEXT, salary INTEGER, active INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO employees VALUES (1, 'Alice', 'engineering', 'senior', 120000, 1)");
        $this->pdo->exec("INSERT INTO employees VALUES (2, 'Bob', 'engineering', 'junior', 70000, 1)");
        $this->pdo->exec("INSERT INTO employees VALUES (3, 'Charlie', 'sales', 'senior', 100000, 1)");
        $this->pdo->exec("INSERT INTO employees VALUES (4, 'Diana', 'sales', 'junior', 55000, 0)");
        $this->pdo->exec("INSERT INTO employees VALUES (5, 'Eve', 'hr', 'senior', 95000, 1)");
        $this->pdo->exec("INSERT INTO employees VALUES (6, 'Frank', 'hr', 'junior', 60000, 0)");
    }

    /**
     * Nested CASE in SELECT: outer CASE branches on department, inner CASE
     * further refines by role within that department.
     */
    public function testNestedCaseInSelect(): void
    {
        $stmt = $this->pdo->query("
            SELECT name,
                   CASE department
                       WHEN 'engineering' THEN
                           CASE WHEN salary >= 100000 THEN 'eng-high'
                                ELSE 'eng-low'
                           END
                       ELSE
                           CASE WHEN role = 'senior' THEN 'other-senior'
                                ELSE 'other-junior'
                           END
                   END AS band
            FROM employees
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(6, $rows);
        $this->assertSame('eng-high', $rows[0]['band']);    // Alice: engineering, 120k
        $this->assertSame('eng-low', $rows[1]['band']);     // Bob: engineering, 70k
        $this->assertSame('other-senior', $rows[2]['band']); // Charlie: sales, senior
        $this->assertSame('other-junior', $rows[3]['band']); // Diana: sales, junior
        $this->assertSame('other-senior', $rows[4]['band']); // Eve: hr, senior
        $this->assertSame('other-junior', $rows[5]['band']); // Frank: hr, junior
    }

    /**
     * Nested CASE in ORDER BY: primary sort by department priority via outer
     * CASE, secondary sort by role seniority via inner CASE.
     */
    public function testNestedCaseInOrderBy(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM employees
            ORDER BY
                CASE department
                    WHEN 'engineering' THEN 1
                    WHEN 'sales' THEN 2
                    ELSE 3
                END,
                CASE department
                    WHEN 'engineering' THEN
                        CASE WHEN salary >= 100000 THEN 1 ELSE 2 END
                    ELSE
                        CASE WHEN role = 'senior' THEN 1 ELSE 2 END
                END,
                name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // engineering first (dept=1): Alice (high salary=1), Bob (low salary=2)
        // sales second (dept=2): Charlie (senior=1), Diana (junior=2)
        // hr third (dept=3): Eve (senior=1), Frank (junior=2)
        $this->assertSame(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank'], $rows);
    }

    /**
     * Nested CASE in UPDATE SET clause: apply different raises based on
     * department and role using nested CASE.
     */
    public function testNestedCaseInUpdateSet(): void
    {
        $this->pdo->exec("
            UPDATE employees SET salary =
                CASE department
                    WHEN 'engineering' THEN
                        CASE WHEN role = 'senior' THEN salary + 10000
                             ELSE salary + 5000
                        END
                    WHEN 'sales' THEN
                        CASE WHEN active = 1 THEN salary + 8000
                             ELSE salary + 3000
                        END
                    ELSE salary + 4000
                END
        ");

        $stmt = $this->pdo->query('SELECT name, salary FROM employees ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(130000, (int) $rows[0]['salary']); // Alice: eng senior +10k
        $this->assertSame(75000, (int) $rows[1]['salary']);   // Bob: eng junior +5k
        $this->assertSame(108000, (int) $rows[2]['salary']); // Charlie: sales active +8k
        $this->assertSame(58000, (int) $rows[3]['salary']);   // Diana: sales inactive +3k
        $this->assertSame(99000, (int) $rows[4]['salary']);   // Eve: hr +4k
        $this->assertSame(64000, (int) $rows[5]['salary']);   // Frank: hr +4k
    }

    /**
     * Multiple WHEN branches in outer CASE with nested inner CASE in one
     * branch: tests that the parser handles depth correctly across sibling
     * WHEN clauses.
     */
    public function testMultipleWhenBranchesWithNestedCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT name,
                   CASE
                       WHEN department = 'engineering' AND role = 'senior' THEN 'lead'
                       WHEN department = 'engineering' AND role = 'junior' THEN 'ic'
                       WHEN department = 'sales' THEN
                           CASE WHEN active = 1 THEN 'field'
                                ELSE 'bench'
                           END
                       WHEN department = 'hr' THEN
                           CASE WHEN salary >= 90000 THEN 'hr-mgr'
                                ELSE 'hr-staff'
                           END
                       ELSE 'unknown'
                   END AS label
            FROM employees
            ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('lead', $rows[0]['label']);      // Alice
        $this->assertSame('ic', $rows[1]['label']);         // Bob
        $this->assertSame('field', $rows[2]['label']);      // Charlie: sales, active
        $this->assertSame('bench', $rows[3]['label']);      // Diana: sales, inactive
        $this->assertSame('hr-mgr', $rows[4]['label']);     // Eve: hr, 95k
        $this->assertSame('hr-staff', $rows[5]['label']);   // Frank: hr, 60k
    }

    /**
     * Prepared statement with nested CASE and bound parameters: ensures
     * parameter binding works correctly when placeholders appear inside
     * nested CASE branches.
     */
    public function testPreparedStatementWithNestedCaseAndParams(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT name,
                   CASE
                       WHEN department = ? THEN
                           CASE WHEN salary >= ? THEN 'target-high'
                                ELSE 'target-low'
                           END
                       ELSE 'other'
                   END AS classification
            FROM employees
            ORDER BY id
        ");
        $stmt->execute(['engineering', 100000]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('target-high', $rows[0]['classification']); // Alice: eng, 120k
        $this->assertSame('target-low', $rows[1]['classification']);   // Bob: eng, 70k
        $this->assertSame('other', $rows[2]['classification']);        // Charlie: sales
        $this->assertSame('other', $rows[3]['classification']);        // Diana: sales
        $this->assertSame('other', $rows[4]['classification']);        // Eve: hr
        $this->assertSame('other', $rows[5]['classification']);        // Frank: hr

        // Re-execute with different parameters to verify reuse
        $stmt->execute(['sales', 80000]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('other', $rows[0]['classification']);       // Alice: eng
        $this->assertSame('other', $rows[1]['classification']);       // Bob: eng
        $this->assertSame('target-high', $rows[2]['classification']); // Charlie: sales, 100k
        $this->assertSame('target-low', $rows[3]['classification']);   // Diana: sales, 55k
        $this->assertSame('other', $rows[4]['classification']);       // Eve: hr
        $this->assertSame('other', $rows[5]['classification']);       // Frank: hr
    }
}
