<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ALTER TABLE ADD COLUMN behavior with shadow store via MySQLi.
 *
 * Cross-platform parity with MysqlAlterTableAfterDataTest (PDO).
 * @spec SPEC-5.1a
 */
class AlterTableAfterDataTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_evolve (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_evolve'];
    }


    /**
     * On MySQL, SELECT with new column works because physical table has it.
     */
    public function testSelectNewColumnWorksOnMysql(): void
    {
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (1, 'Alice')");
        $this->mysqli->query('ALTER TABLE mi_evolve ADD COLUMN score INT');

        $result = $this->mysqli->query('SELECT name, score FROM mi_evolve WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testInsertWithNewColumnSucceeds(): void
    {
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (1, 'Alice')");
        $this->mysqli->query('ALTER TABLE mi_evolve ADD COLUMN score INT');

        $this->mysqli->query("INSERT INTO mi_evolve (id, name, score) VALUES (2, 'Bob', 100)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_evolve');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    public function testOriginalColumnsStillWorkAfterAlter(): void
    {
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_evolve VALUES (2, 'Bob')");
        $this->mysqli->query('ALTER TABLE mi_evolve ADD COLUMN score INT');

        $result = $this->mysqli->query('SELECT name FROM mi_evolve WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }
}
