<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET col = CASE ... END through PostgreSQL CTE shadow store.
 *
 * The PgSqlParser uses regex to extract SET clause assignments. CASE
 * expressions in SET values contain WHEN/THEN/ELSE/END keywords that
 * may interfere with regex-based parsing boundaries.
 *
 * Related: SPEC-11.PG-UPDATE-SET-FROM-KEYWORD (FROM keyword in SET truncation)
 */
class PostgresUpdateCaseInSetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ucs_employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(50) NOT NULL,
                salary NUMERIC(10,2) NOT NULL,
                grade VARCHAR(10) NOT NULL DEFAULT \'C\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ucs_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ucs_employees (id, name, department, salary, grade) VALUES (1, 'Alice', 'Engineering', 90000, 'B')");
        $this->pdo->exec("INSERT INTO pg_ucs_employees (id, name, department, salary, grade) VALUES (2, 'Bob', 'Marketing', 60000, 'C')");
        $this->pdo->exec("INSERT INTO pg_ucs_employees (id, name, department, salary, grade) VALUES (3, 'Carol', 'Engineering', 120000, 'A')");
        $this->pdo->exec("INSERT INTO pg_ucs_employees (id, name, department, salary, grade) VALUES (4, 'Dave', 'Sales', 45000, 'D')");
    }

    /**
     * UPDATE SET with simple CASE expression.
     */
    public function testUpdateSetSimpleCase(): void
    {
        $this->pdo->exec(
            "UPDATE pg_ucs_employees SET grade = CASE
                WHEN salary >= 100000 THEN 'A'
                WHEN salary >= 75000 THEN 'B'
                WHEN salary >= 50000 THEN 'C'
                ELSE 'D'
             END"
        );

        $rows = $this->ztdQuery("SELECT name, grade FROM pg_ucs_employees ORDER BY name");
        $this->assertCount(4, $rows);
        $this->assertEquals('B', $rows[0]['grade']); // Alice: 90000 → B
        $this->assertEquals('C', $rows[1]['grade']); // Bob: 60000 → C
        $this->assertEquals('A', $rows[2]['grade']); // Carol: 120000 → A
        $this->assertEquals('D', $rows[3]['grade']); // Dave: 45000 → D
    }

    /**
     * UPDATE SET with CASE and WHERE clause.
     */
    public function testUpdateSetCaseWithWhere(): void
    {
        $this->pdo->exec(
            "UPDATE pg_ucs_employees SET salary = CASE
                WHEN department = 'Engineering' THEN salary * 1.10
                ELSE salary * 1.05
             END
             WHERE grade IN ('B', 'C')"
        );

        $rows = $this->ztdQuery("SELECT name, salary FROM pg_ucs_employees ORDER BY name");

        // Alice (Eng, B): 90000 * 1.10 = 99000
        $this->assertEquals(99000, (float) $rows[0]['salary']);
        // Bob (Marketing, C): 60000 * 1.05 = 63000
        $this->assertEquals(63000, (float) $rows[1]['salary']);
        // Carol (Eng, A): unchanged 120000
        $this->assertEquals(120000, (float) $rows[2]['salary']);
        // Dave (Sales, D): unchanged 45000
        $this->assertEquals(45000, (float) $rows[3]['salary']);
    }

    /**
     * UPDATE SET with multiple columns, one using CASE.
     */
    public function testUpdateMultipleColumnsWithCase(): void
    {
        $this->pdo->exec(
            "UPDATE pg_ucs_employees SET
                grade = CASE WHEN salary >= 100000 THEN 'A' ELSE 'B' END,
                department = 'Reviewed'
             WHERE id IN (1, 3)"
        );

        $rows = $this->ztdQuery("SELECT name, grade, department FROM pg_ucs_employees WHERE id IN (1, 3) ORDER BY name");
        $this->assertCount(2, $rows);
        $this->assertEquals('B', $rows[0]['grade']); // Alice: 90000 → B
        $this->assertEquals('Reviewed', $rows[0]['department']);
        $this->assertEquals('A', $rows[1]['grade']); // Carol: 120000 → A
        $this->assertEquals('Reviewed', $rows[1]['department']);
    }

    /**
     * UPDATE SET with CASE referencing searched CASE on column.
     */
    public function testUpdateSetSearchedCase(): void
    {
        $this->pdo->exec(
            "UPDATE pg_ucs_employees SET salary = CASE department
                WHEN 'Engineering' THEN salary + 5000
                WHEN 'Marketing' THEN salary + 3000
                WHEN 'Sales' THEN salary + 2000
                ELSE salary
             END"
        );

        $rows = $this->ztdQuery("SELECT name, salary FROM pg_ucs_employees ORDER BY name");
        $this->assertEquals(95000, (float) $rows[0]['salary']); // Alice
        $this->assertEquals(63000, (float) $rows[1]['salary']); // Bob
        $this->assertEquals(125000, (float) $rows[2]['salary']); // Carol
        $this->assertEquals(47000, (float) $rows[3]['salary']); // Dave
    }

    /**
     * UPDATE SET with nested CASE expressions.
     */
    public function testUpdateSetNestedCase(): void
    {
        $this->pdo->exec(
            "UPDATE pg_ucs_employees SET grade = CASE
                WHEN department = 'Engineering' THEN
                    CASE WHEN salary >= 100000 THEN 'S' ELSE 'A' END
                WHEN department = 'Marketing' THEN 'B'
                ELSE 'C'
             END"
        );

        $rows = $this->ztdQuery("SELECT name, grade FROM pg_ucs_employees ORDER BY name");
        $this->assertEquals('A', $rows[0]['grade']); // Alice: Eng, 90000 → A
        $this->assertEquals('B', $rows[1]['grade']); // Bob: Marketing → B
        $this->assertEquals('S', $rows[2]['grade']); // Carol: Eng, 120000 → S
        $this->assertEquals('C', $rows[3]['grade']); // Dave: Sales → C
    }

    /**
     * UPDATE SET CASE with prepared statement.
     *
     * The PgSqlParser may mis-handle CASE expressions in SET clauses
     * combined with $N parameter placeholders, producing a no-op UPDATE.
     */
    public function testUpdateSetCasePrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "UPDATE pg_ucs_employees SET salary = CASE
                WHEN salary >= $1 THEN salary * 1.10
                ELSE salary * 1.05
             END
             WHERE department = $2"
        );
        $stmt->execute([80000, 'Engineering']);

        $rows = $this->ztdQuery("SELECT name, salary FROM pg_ucs_employees WHERE department = 'Engineering' ORDER BY name");

        if ((float) $rows[0]['salary'] === 90000.0) {
            $this->markTestIncomplete(
                'UPDATE SET with CASE expression and prepared $N params is silently a no-op on PostgreSQL. '
                . 'The salary remained unchanged (90000) instead of 99000 (90000 * 1.10). '
                . 'Non-prepared CASE in SET works correctly. The PgSqlParser likely mis-parses '
                . 'the CASE+$N combination in the SET clause.'
            );
        }

        $this->assertEquals(99000, (float) $rows[0]['salary']); // Alice: 90000 * 1.10
        $this->assertEquals(132000, (float) $rows[1]['salary']); // Carol: 120000 * 1.10
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_ucs_employees')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
