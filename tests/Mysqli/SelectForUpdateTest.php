<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests SELECT...FOR UPDATE locking clause on MySQLi.
 *
 * FOR UPDATE is preserved in CTE-rewritten SQL but is effectively a no-op
 * since the query reads from CTE data, not physical rows.
 * @spec SPEC-10.2.11
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
        $row = $result->fetch_assoc();
        $this->assertSame('Bob', $row['name']);
    }

    /**
     * SELECT...LOCK IN SHARE MODE (MySQL-specific) works.
     */
    public function testLockInShareModeReturnsData(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_fu_test WHERE id = 1 LOCK IN SHARE MODE');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Physical isolation maintained.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_fu_test');
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }
}
