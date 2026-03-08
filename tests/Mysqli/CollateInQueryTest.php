<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests COLLATE clause in queries via MySQLi.
 *
 * Cross-platform parity with MysqlCollateInQueryTest (PDO).
 * @spec pending
 */
class CollateInQueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_collate_test (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            code VARCHAR(20)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci';
    }

    protected function getTableNames(): array
    {
        return ['mi_collate_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_collate_test VALUES (1, 'Alice', 'abc')");
        $this->mysqli->query("INSERT INTO mi_collate_test VALUES (2, 'alice', 'ABC')");
        $this->mysqli->query("INSERT INTO mi_collate_test VALUES (3, 'Bob', 'def')");
        $this->mysqli->query("INSERT INTO mi_collate_test VALUES (4, 'CHARLIE', 'GHI')");
    }

    /**
     * WHERE with COLLATE for case-sensitive comparison.
     */
    public function testWhereCollateBinary(): void
    {
        $result = $this->mysqli->query("SELECT name FROM mi_collate_test WHERE name COLLATE utf8mb4_bin = 'alice'");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('alice', $rows[0]['name']);
    }

    /**
     * ORDER BY with COLLATE for case-sensitive sorting.
     */
    public function testOrderByCollateBinary(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_collate_test ORDER BY name COLLATE utf8mb4_bin');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        // Binary: uppercase before lowercase
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('CHARLIE', $rows[2]['name']);
        $this->assertSame('alice', $rows[3]['name']);
    }

    /**
     * COLLATE after mutation.
     */
    public function testCollateAfterMutation(): void
    {
        $this->mysqli->query("INSERT INTO mi_collate_test VALUES (5, 'ALICE', 'xyz')");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_collate_test WHERE name COLLATE utf8mb4_bin = 'ALICE'");
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_collate_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
