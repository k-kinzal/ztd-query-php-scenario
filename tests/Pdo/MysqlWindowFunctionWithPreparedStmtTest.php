<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests window functions with prepared statements on MySQL.
 *
 * Cross-platform parity with SqliteWindowFunctionWithPreparedStmtTest.
 * @spec SPEC-10.2.23
 */
class MysqlWindowFunctionWithPreparedStmtTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_wfprep_test (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(50), salary INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_wfprep_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_wfprep_test VALUES (2, 'Bob', 'Engineering', 85000)");
        $this->pdo->exec("INSERT INTO pdo_wfprep_test VALUES (3, 'Charlie', 'Sales', 70000)");
        $this->pdo->exec("INSERT INTO pdo_wfprep_test VALUES (4, 'Diana', 'Sales', 75000)");
        $this->pdo->exec("INSERT INTO pdo_wfprep_test VALUES (5, 'Eve', 'Engineering', 95000)");
    }

    /**
     * ROW_NUMBER with WHERE parameter.
     */
    public function testRowNumberWithWhereParam(): void
    {
        $stmt = $this->pdo->prepare('
            SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
            FROM pdo_wfprep_test
            WHERE dept = ?
        ');
        $stmt->execute(['Engineering']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['rn']);
    }

    /**
     * Multiple window functions.
     */
    public function testMultipleWindowFunctions(): void
    {
        $stmt = $this->pdo->prepare('
            SELECT name,
                   ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
                   RANK() OVER (ORDER BY salary DESC) AS rnk
            FROM pdo_wfprep_test
            WHERE dept = ?
        ');
        $stmt->execute(['Engineering']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }

    /**
     * Window function after INSERT mutation.
     */
    public function testWindowFunctionAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pdo_wfprep_test VALUES (6, 'Frank', 'Engineering', 100000)");

        $stmt = $this->pdo->prepare('
            SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
            FROM pdo_wfprep_test
            WHERE dept = ?
        ');
        $stmt->execute(['Engineering']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_wfprep_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
