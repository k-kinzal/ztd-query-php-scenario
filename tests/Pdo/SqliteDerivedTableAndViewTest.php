<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests derived tables (subqueries in FROM), views, and INSERT with defaults on SQLite.
 * These patterns stress the CTE rewriter's ability to handle non-standard table references.
 * @spec SPEC-3.3a
 */
class SqliteDerivedTableAndViewTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary INTEGER)',
            'CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget INTEGER)',
            'CREATE TABLE auto_table (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT DEFAULT \\\'default_val\\\')',
            'CREATE TABLE config (id INTEGER PRIMARY KEY, key TEXT NOT NULL, value TEXT DEFAULT \\\'empty\\\', active INTEGER DEFAULT 1)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['employees', 'departments', 'auto_table', 'config'];
    }


    /**
     * Derived table as the sole FROM source — CTE rewriter does NOT rewrite table
     * references inside the derived subquery, so it reads from the physical table (empty).
     */
    public function testDerivedTableInFromReturnsEmpty(): void
    {
        $stmt = $this->pdo->query("
            SELECT sub.department, sub.avg_salary
            FROM (
                SELECT department, AVG(salary) AS avg_salary
                FROM employees
                GROUP BY department
            ) AS sub
            WHERE sub.avg_salary > 90000
            ORDER BY sub.department
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Returns empty because CTE rewriter doesn't handle derived tables in FROM
        $this->assertCount(0, $rows);
    }

    /**
     * Derived table JOINed with a regular table — works correctly because the CTE
     * rewriter processes the regular table reference and also rewrites references
     * inside the derived subquery in this context.
     */
    public function testDerivedTableWithJoinWorks(): void
    {
        $stmt = $this->pdo->query("
            SELECT d.name AS dept, sub.emp_count
            FROM departments d
            JOIN (
                SELECT department, COUNT(*) AS emp_count
                FROM employees
                GROUP BY department
            ) AS sub ON d.name = sub.department
            ORDER BY d.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['dept']);
        $this->assertSame(2, (int) $rows[0]['emp_count']);
    }

    /**
     * Nested derived tables — same limitation as single derived table in FROM.
     */
    public function testNestedDerivedTablesReturnEmpty(): void
    {
        $stmt = $this->pdo->query("
            SELECT outer_sub.department
            FROM (
                SELECT department, total
                FROM (
                    SELECT department, SUM(salary) AS total
                    FROM employees
                    GROUP BY department
                ) AS inner_sub
                WHERE total > 180000
            ) AS outer_sub
            ORDER BY outer_sub.department
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    /**
     * Derived table with prepared statement — same limitation.
     */
    public function testDerivedTableWithPreparedStatementReturnsEmpty(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT sub.department, sub.cnt
            FROM (
                SELECT department, COUNT(*) AS cnt
                FROM employees
                WHERE salary > ?
                GROUP BY department
            ) AS sub
            WHERE sub.cnt >= ?
            ORDER BY sub.department
        ");
        $stmt->execute([90000, 1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    /**
     * Views — behave similarly to derived tables. The view reads from the physical
     * table (which has no data in shadow mode), so it returns empty.
     */
    public function testViewReturnsEmptyWithZtd(): void
    {
        $stmt = $this->pdo->query("SELECT * FROM dept_summary ORDER BY department");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    /**
     * INSERT DEFAULT VALUES — not supported by ZTD. The InsertTransformer requires
     * explicit values to project into the CTE.
     */
    public function testInsertDefaultValuesFails(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE auto_table (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT DEFAULT \'default_val\')');

        $pdo = ZtdPdo::fromPdo($raw);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Insert statement has no values to project');
        $pdo->exec("INSERT INTO auto_table DEFAULT VALUES");
    }

    public function testInsertWithPartialColumnsAndDefaults(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE config (id INTEGER PRIMARY KEY, key TEXT NOT NULL, value TEXT DEFAULT \'empty\', active INTEGER DEFAULT 1)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO config (id, key) VALUES (1, 'theme')");

        $stmt = $pdo->query('SELECT * FROM config WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('theme', $row['key']);
    }

    /**
     * UPDATE with derived table subquery in WHERE — CTE rewriter produces
     * "incomplete input" error (same class of bug as other UPDATE+subquery issues).
     */
    public function testDerivedTableInUpdateSubqueryFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec("
            UPDATE employees SET salary = salary + 10000
            WHERE department IN (
                SELECT sub.department FROM (
                    SELECT department, AVG(salary) AS avg_sal
                    FROM employees
                    GROUP BY department
                ) AS sub
                WHERE sub.avg_sal < 100000
            )
        ");
    }

    /**
     * Direct query on derived table joined with regular table works after mutations.
     * Verifies that data mutations are visible through derived table JOINs.
     */
    public function testDerivedTableWithJoinReflectsMutations(): void
    {
        $this->pdo->exec("UPDATE employees SET salary = 200000 WHERE name = 'Charlie'");

        $stmt = $this->pdo->query("
            SELECT d.name AS dept, sub.avg_salary
            FROM departments d
            JOIN (
                SELECT department, AVG(salary) AS avg_salary
                FROM employees
                GROUP BY department
            ) AS sub ON d.name = sub.department
            ORDER BY d.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);

        // Marketing: Charlie(200k) + Diana(85k) = avg 142.5k
        $marketing = array_values(array_filter($rows, fn($r) => $r['dept'] === 'Marketing'));
        $this->assertEqualsWithDelta(142500, (float) $marketing[0]['avg_salary'], 1);
    }
}
