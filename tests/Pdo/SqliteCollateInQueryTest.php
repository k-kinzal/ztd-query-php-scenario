<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests COLLATE clause in queries with shadow data on SQLite.
 *
 * SQLite supports BINARY, NOCASE, and RTRIM collations.
 */
class SqliteCollateInQueryTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_collate_test (id INTEGER PRIMARY KEY, name TEXT, code TEXT)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO sl_collate_test VALUES (1, 'Alice', 'abc')");
        $this->pdo->exec("INSERT INTO sl_collate_test VALUES (2, 'alice', 'ABC')");
        $this->pdo->exec("INSERT INTO sl_collate_test VALUES (3, 'Bob', 'def')");
        $this->pdo->exec("INSERT INTO sl_collate_test VALUES (4, 'CHARLIE', 'GHI')");
        $this->pdo->exec("INSERT INTO sl_collate_test VALUES (5, 'charlie', 'ghi')");
    }

    /**
     * WHERE with COLLATE NOCASE for case-insensitive comparison.
     */
    public function testWhereCollateNocase(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM sl_collate_test WHERE name = 'alice' COLLATE NOCASE ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // NOCASE: both 'Alice' and 'alice'
        $this->assertCount(2, $rows);
    }

    /**
     * WHERE with COLLATE BINARY for case-sensitive comparison.
     */
    public function testWhereCollateBinary(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM sl_collate_test WHERE name = 'alice' COLLATE BINARY");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertSame('alice', $rows[0]);
    }

    /**
     * ORDER BY with COLLATE NOCASE.
     */
    public function testOrderByCollateNocase(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM sl_collate_test ORDER BY name COLLATE NOCASE');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(5, $rows);
        // Case-insensitive sort groups: alice/Alice, bob, charlie/CHARLIE
    }

    /**
     * ORDER BY with COLLATE BINARY for strict byte-order.
     */
    public function testOrderByCollateBinary(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM sl_collate_test ORDER BY name COLLATE BINARY');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // BINARY: uppercase before lowercase
        $this->assertSame('Alice', $rows[0]);
        $this->assertSame('Bob', $rows[1]);
        $this->assertSame('CHARLIE', $rows[2]);
        $this->assertSame('alice', $rows[3]);
        $this->assertSame('charlie', $rows[4]);
    }

    /**
     * COLLATE after mutation.
     */
    public function testCollateAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_collate_test VALUES (6, 'ALICE', 'xyz')");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM sl_collate_test WHERE name = 'ALICE' COLLATE BINARY");
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_collate_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
