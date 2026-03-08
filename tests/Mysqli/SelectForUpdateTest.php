<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests SELECT...FOR UPDATE locking clause via MySQLi.
 *
 * Cross-platform parity with MysqlSelectForUpdateTest (PDO).
 * @spec pending
 */
class SelectForUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_fu_test (id INT PRIMARY KEY, name VARCHAR(50), balance INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_fu_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_fu_test VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_fu_test VALUES (2, 'Bob', 200)");
    }

    /**
     * SELECT...FOR UPDATE returns correct shadow data.
     */
    public function testSelectForUpdateReturnsData(): void
    {
        $result = $this->mysqli->query('SELECT name, balance FROM mi_fu_test WHERE id = 1 FOR UPDATE');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['balance']);
    }

    /**
     * SELECT...FOR SHARE also works.
     */
    public function testSelectForShareReturnsData(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_fu_test WHERE id = 2 FOR SHARE');
        $this->assertSame('Bob', $result->fetch_assoc()['name']);
    }

    /**
     * Physical isolation maintained.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_fu_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
