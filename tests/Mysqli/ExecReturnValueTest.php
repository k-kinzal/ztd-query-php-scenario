<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests lastAffectedRows() accuracy across various operations on MySQLi ZTD.
 * @spec SPEC-4.4
 */
class ExecReturnValueTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_rv (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_rv'];
    }


    public function testLastAffectedRowsAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsAfterMultiRowInsert(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)"
        );
        $this->assertSame(3, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsAfterUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");
        $this->mysqli->query("UPDATE mi_rv SET score = 999 WHERE active = 1");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsAfterDelete(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 0)");
        $this->mysqli->query("DELETE FROM mi_rv WHERE active = 0");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }

    public function testLastAffectedRowsNoMatch(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1)");
        $this->mysqli->query("UPDATE mi_rv SET score = 999 WHERE id = 999");
        $this->assertSame(0, $this->mysqli->lastAffectedRows());
    }

    public function testZtdAffectedRowsOnPreparedUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");

        $stmt = $this->mysqli->prepare('UPDATE mi_rv SET score = ? WHERE active = ?');
        $score = 999;
        $active = 1;
        $stmt->bind_param('ii', $score, $active);
        $stmt->execute();

        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testZtdAffectedRowsOnPreparedDelete(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 0)");

        $stmt = $this->mysqli->prepare('DELETE FROM mi_rv WHERE active = ?');
        $active = 0;
        $stmt->bind_param('i', $active);
        $stmt->execute();

        $this->assertSame(1, $stmt->ztdAffectedRows());
    }

    public function testSequentialAffectedRows(): void
    {
        $this->mysqli->query("INSERT INTO mi_rv VALUES (1, 'A', 10, 1)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("INSERT INTO mi_rv VALUES (2, 'B', 20, 1)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("UPDATE mi_rv SET score = 999 WHERE active = 1");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("DELETE FROM mi_rv WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }
}
