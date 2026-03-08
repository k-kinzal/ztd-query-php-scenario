<?php

declare(strict_types=1);

namespace Tests\Scenarios;

/**
 * Shared JOIN and subquery scenario for all platforms.
 *
 * Requires tables:
 *   - users (id INT/INTEGER PRIMARY KEY, name VARCHAR/TEXT, dept_id INT/INTEGER)
 *   - departments (id INT/INTEGER PRIMARY KEY, dept_name VARCHAR/TEXT)
 * Provided by the concrete test class via getTableDDL().
 */
trait JoinAndSubqueryScenario
{
    abstract protected function ztdExec(string $sql): int|false;
    abstract protected function ztdQuery(string $sql): array;

    protected function seedJoinData(): void
    {
        $this->ztdExec("INSERT INTO departments (id, dept_name) VALUES (1, 'Engineering')");
        $this->ztdExec("INSERT INTO departments (id, dept_name) VALUES (2, 'Marketing')");
        $this->ztdExec("INSERT INTO users (id, name, dept_id) VALUES (1, 'Alice', 1)");
        $this->ztdExec("INSERT INTO users (id, name, dept_id) VALUES (2, 'Bob', 1)");
        $this->ztdExec("INSERT INTO users (id, name, dept_id) VALUES (3, 'Charlie', 2)");
        $this->ztdExec("INSERT INTO users (id, name, dept_id) VALUES (4, 'Diana', NULL)");
    }

    public function testInnerJoin(): void
    {
        $this->seedJoinData();

        $rows = $this->ztdQuery(
            'SELECT u.name, d.dept_name FROM users u '
            . 'INNER JOIN departments d ON u.dept_id = d.id '
            . 'ORDER BY u.name'
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertSame('Marketing', $rows[2]['dept_name']);
    }

    public function testLeftJoin(): void
    {
        $this->seedJoinData();

        $rows = $this->ztdQuery(
            'SELECT u.name, d.dept_name FROM users u '
            . 'LEFT JOIN departments d ON u.dept_id = d.id '
            . 'ORDER BY u.name'
        );

        $this->assertCount(4, $rows);
        // Diana has no department
        $diana = array_values(array_filter($rows, fn($r) => $r['name'] === 'Diana'));
        $this->assertCount(1, $diana);
        $this->assertNull($diana[0]['dept_name']);
    }

    public function testSubqueryInWhere(): void
    {
        $this->seedJoinData();

        $rows = $this->ztdQuery(
            'SELECT name FROM users WHERE dept_id IN '
            . "(SELECT id FROM departments WHERE dept_name = 'Engineering') "
            . 'ORDER BY name'
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testScalarSubqueryInSelect(): void
    {
        $this->seedJoinData();

        $rows = $this->ztdQuery(
            'SELECT d.dept_name, '
            . '(SELECT COUNT(*) FROM users u WHERE u.dept_id = d.id) AS user_count '
            . 'FROM departments d ORDER BY d.dept_name'
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame(2, (int) $rows[0]['user_count']);
        $this->assertSame('Marketing', $rows[1]['dept_name']);
        $this->assertSame(1, (int) $rows[1]['user_count']);
    }

    public function testExistsSubquery(): void
    {
        $this->seedJoinData();

        $rows = $this->ztdQuery(
            'SELECT d.dept_name FROM departments d '
            . 'WHERE EXISTS (SELECT 1 FROM users u WHERE u.dept_id = d.id) '
            . 'ORDER BY d.dept_name'
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Marketing', $rows[1]['dept_name']);
    }

    public function testJoinWithAggregation(): void
    {
        $this->seedJoinData();

        $rows = $this->ztdQuery(
            'SELECT d.dept_name, COUNT(u.id) AS cnt '
            . 'FROM departments d '
            . 'LEFT JOIN users u ON u.dept_id = d.id '
            . 'GROUP BY d.dept_name '
            . 'ORDER BY d.dept_name'
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }
}
