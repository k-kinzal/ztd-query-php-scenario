<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests same table referenced multiple times with different aliases.
 * The CTE rewriter must produce a single CTE for the table and correctly
 * resolve all aliases pointing to it.
 * @spec SPEC-3.3
 */
class PostgresMultiAliasJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_maj_employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                manager_id INTEGER,
                mentor_id INTEGER,
                dept TEXT,
                salary INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_maj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_maj_employees VALUES (1, 'Alice',   NULL, NULL, 'exec', 200)");
        $this->pdo->exec("INSERT INTO pg_maj_employees VALUES (2, 'Bob',     1,    NULL, 'exec', 150)");
        $this->pdo->exec("INSERT INTO pg_maj_employees VALUES (3, 'Charlie', 2,    1,    'eng',  120)");
        $this->pdo->exec("INSERT INTO pg_maj_employees VALUES (4, 'Diana',   2,    1,    'eng',  110)");
        $this->pdo->exec("INSERT INTO pg_maj_employees VALUES (5, 'Eve',     3,    2,    'eng',  100)");
        $this->pdo->exec("INSERT INTO pg_maj_employees VALUES (6, 'Frank',   3,    2,    'eng',   95)");
    }

    /**
     * Basic self-join: employee with manager name.
     */
    public function testSelfJoinTwoAliases(): void
    {
        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM pg_maj_employees a
            JOIN pg_maj_employees b ON a.manager_id = b.id
            ORDER BY a.name
        ");
        $this->assertCount(5, $rows);
        $this->assertSame('Bob', $rows[0]['employee']);
        $this->assertSame('Alice', $rows[0]['manager']);
        $this->assertSame('Diana', $rows[2]['employee']);
        $this->assertSame('Bob', $rows[2]['manager']);
    }

    /**
     * Triple self-join: employee, manager, and mentor from the same table.
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                e.name AS employee,
                m.name AS manager,
                t.name AS mentor
            FROM pg_maj_employees e
            LEFT JOIN pg_maj_employees m ON e.manager_id = m.id
            LEFT JOIN pg_maj_employees t ON e.mentor_id = t.id
            ORDER BY e.id
        ");
        $this->assertCount(6, $rows);

        $this->assertSame('Alice', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
        $this->assertNull($rows[0]['mentor']);

        $this->assertSame('Charlie', $rows[2]['employee']);
        $this->assertSame('Bob', $rows[2]['manager']);
        $this->assertSame('Alice', $rows[2]['mentor']);

        $this->assertSame('Eve', $rows[4]['employee']);
        $this->assertSame('Charlie', $rows[4]['manager']);
        $this->assertSame('Bob', $rows[4]['mentor']);
    }

    /**
     * Self-join after UPDATE.
     */
    public function testSelfJoinAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE pg_maj_employees SET name = 'Charles', salary = 160 WHERE id = 3");

        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM pg_maj_employees a
            JOIN pg_maj_employees b ON a.manager_id = b.id
            WHERE a.id = 5
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['employee']);
        $this->assertSame('Charles', $rows[0]['manager']);
    }

    /**
     * Self-join after INSERT.
     */
    public function testSelfJoinAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_maj_employees VALUES (7, 'Grace', 5, 3, 'eng', 90)");

        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM pg_maj_employees a
            JOIN pg_maj_employees b ON a.manager_id = b.id
            WHERE a.id = 7
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Grace', $rows[0]['employee']);
        $this->assertSame('Eve', $rows[0]['manager']);
    }

    /**
     * Self-join with aggregation.
     */
    public function testSelfJoinWithAggregation(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                m.name AS manager,
                COUNT(e.id) AS report_count,
                SUM(e.salary) AS total_report_salary
            FROM pg_maj_employees e
            JOIN pg_maj_employees m ON e.manager_id = m.id
            GROUP BY m.id, m.name
            ORDER BY report_count DESC, m.name
        ");
        $this->assertCount(3, $rows);

        $byManager = array_column($rows, null, 'manager');
        $this->assertEquals(2, (int) $byManager['Bob']['report_count']);
        $this->assertEquals(230, (int) $byManager['Bob']['total_report_salary']);
        $this->assertEquals(2, (int) $byManager['Charlie']['report_count']);
        $this->assertEquals(195, (int) $byManager['Charlie']['total_report_salary']);
        $this->assertEquals(1, (int) $byManager['Alice']['report_count']);
    }

    /**
     * Self-join with WHERE filter on both aliases.
     */
    public function testSelfJoinFilterBothAliases(): void
    {
        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM pg_maj_employees a
            JOIN pg_maj_employees b ON a.manager_id = b.id
            WHERE a.dept = 'eng' AND b.dept = 'exec'
            ORDER BY a.name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['employee']);
        $this->assertSame('Bob', $rows[0]['manager']);
        $this->assertSame('Diana', $rows[1]['employee']);
        $this->assertSame('Bob', $rows[1]['manager']);
    }

    /**
     * Self-join salary comparison.
     */
    public function testSelfJoinSalaryComparison(): void
    {
        $rows = $this->ztdQuery("
            SELECT a.name AS employee, a.salary AS emp_salary,
                   b.name AS manager, b.salary AS mgr_salary
            FROM pg_maj_employees a
            JOIN pg_maj_employees b ON a.manager_id = b.id
            WHERE a.salary > b.salary
            ORDER BY a.name
        ");
        $this->assertCount(0, $rows);
    }

    /**
     * Self-join after DELETE.
     */
    public function testSelfJoinAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM pg_maj_employees WHERE id = 3");

        $rows = $this->ztdQuery("
            SELECT a.name AS employee, b.name AS manager
            FROM pg_maj_employees a
            JOIN pg_maj_employees b ON a.manager_id = b.id
            ORDER BY a.name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['employee']);
        $this->assertSame('Diana', $rows[1]['employee']);
    }

    /**
     * Four aliases of the same table.
     */
    public function testQuadrupleAliasSelfJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                e.name AS employee,
                m.name AS manager,
                mm.name AS grand_manager,
                t.name AS mentor
            FROM pg_maj_employees e
            LEFT JOIN pg_maj_employees m ON e.manager_id = m.id
            LEFT JOIN pg_maj_employees mm ON m.manager_id = mm.id
            LEFT JOIN pg_maj_employees t ON e.mentor_id = t.id
            WHERE e.id = 5
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['employee']);
        $this->assertSame('Charlie', $rows[0]['manager']);
        $this->assertSame('Bob', $rows[0]['grand_manager']);
        $this->assertSame('Bob', $rows[0]['mentor']);
    }
}
