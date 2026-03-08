<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DELETE without WHERE clause via MySQLi.
 *
 * Cross-platform parity with MysqlDeleteWithoutWhereTest (PDO).
 * @spec SPEC-4.3
 */
class DeleteWithoutWhereTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_dww_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_dww_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_dww_test VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_dww_test VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_dww_test VALUES (3, 'Charlie')");
    }

    /**
     * DELETE without WHERE works correctly on MySQL.
     */
    public function testDeleteWithoutWhereWorks(): void
    {
        $this->mysqli->query('DELETE FROM mi_dww_test');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dww_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * DELETE with WHERE 1=1 also works.
     */
    public function testDeleteWithWhereTrueWorks(): void
    {
        $this->mysqli->query('DELETE FROM mi_dww_test WHERE 1=1');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dww_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('DELETE FROM mi_dww_test');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dww_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
