<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests window functions with prepared statements via MySQLi.
 *
 * Cross-platform parity with MysqlWindowFunctionWithPreparedStmtTest (PDO).
 * @spec pending
 */
class WindowFunctionWithPreparedStmtTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_wfprep_test (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(50), salary INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_wfprep_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_wfprep_test VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->mysqli->query("INSERT INTO mi_wfprep_test VALUES (2, 'Bob', 'Engineering', 85000)");
        $this->mysqli->query("INSERT INTO mi_wfprep_test VALUES (3, 'Charlie', 'Sales', 70000)");
        $this->mysqli->query("INSERT INTO mi_wfprep_test VALUES (4, 'Diana', 'Sales', 75000)");
        $this->mysqli->query("INSERT INTO mi_wfprep_test VALUES (5, 'Eve', 'Engineering', 95000)");
    }

    /**
     * ROW_NUMBER with prepared WHERE parameter.
     */
    public function testRowNumberWithPreparedWhere(): void
    {
        $stmt = $this->mysqli->prepare('
            SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
            FROM mi_wfprep_test
            WHERE dept = ?
        ');
        $dept = 'Engineering';
        $stmt->bind_param('s', $dept);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['rn']);
    }

    /**
     * Multiple window functions with prepared statement.
     */
    public function testMultipleWindowFunctions(): void
    {
        $stmt = $this->mysqli->prepare('
            SELECT name,
                   ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
                   RANK() OVER (ORDER BY salary DESC) AS rnk
            FROM mi_wfprep_test
            WHERE dept = ?
        ');
        $dept = 'Engineering';
        $stmt->bind_param('s', $dept);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
    }

    /**
     * Window function after INSERT mutation.
     */
    public function testWindowFunctionAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_wfprep_test VALUES (6, 'Frank', 'Engineering', 100000)");

        $stmt = $this->mysqli->prepare('
            SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
            FROM mi_wfprep_test
            WHERE dept = ?
        ');
        $dept = 'Engineering';
        $stmt->bind_param('s', $dept);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_wfprep_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
