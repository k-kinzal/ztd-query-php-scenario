<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests using reserved SQL keywords as column/table names through ZTD.
 *
 * Verifies that identifier quoting handles reserved keywords correctly.
 */
class SqliteReservedKeywordIdentifierTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE "order" (id INT PRIMARY KEY, "select" VARCHAR(50), "from" INT, "where" VARCHAR(50))');
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * INSERT with reserved keyword column names.
     */
    public function testInsertWithReservedKeywords(): void
    {
        $this->pdo->exec("INSERT INTO \"order\" (id, \"select\", \"from\", \"where\") VALUES (1, 'value1', 100, 'cond1')");

        $stmt = $this->pdo->query('SELECT * FROM "order" WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('value1', $row['select']);
        $this->assertEquals(100, (int) $row['from']);
    }

    /**
     * UPDATE with reserved keyword columns.
     */
    public function testUpdateReservedKeywords(): void
    {
        $this->pdo->exec("INSERT INTO \"order\" VALUES (1, 'old', 50, 'old_cond')");
        $this->pdo->exec("UPDATE \"order\" SET \"select\" = 'new' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT "select" FROM "order" WHERE id = 1');
        $this->assertSame('new', $stmt->fetchColumn());
    }

    /**
     * DELETE with reserved keyword in WHERE.
     */
    public function testDeleteWithReservedKeywords(): void
    {
        $this->pdo->exec("INSERT INTO \"order\" VALUES (1, 'a', 1, 'x')");
        $this->pdo->exec("INSERT INTO \"order\" VALUES (2, 'b', 2, 'y')");

        $this->pdo->exec("DELETE FROM \"order\" WHERE \"from\" = 1");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM "order"');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Prepared statements with reserved keywords.
     */
    public function testPreparedWithReservedKeywords(): void
    {
        $this->pdo->exec("INSERT INTO \"order\" VALUES (1, 'val', 10, 'cond')");

        $stmt = $this->pdo->prepare('SELECT "select", "from" FROM "order" WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('val', $row['select']);
    }

    /**
     * Column aliasing with reserved keywords.
     */
    public function testAliasWithReservedKeywords(): void
    {
        $this->pdo->exec("INSERT INTO \"order\" VALUES (1, 'test', 42, 'xyz')");

        $stmt = $this->pdo->query('SELECT "select" AS sel_val, "from" AS from_val FROM "order" WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('test', $row['sel_val']);
        $this->assertEquals(42, (int) $row['from_val']);
    }
}
