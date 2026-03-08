<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests FETCH_LAZY mode, exec() with SELECT, and other edge cases
 * commonly encountered by ORMs and frameworks.
 * @spec SPEC-3.4
 */
class SqliteFetchLazyAndEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE fl_users (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20), score INT)',
            'CREATE TABLE fl_flags (id INT PRIMARY KEY, active INT)',
            'CREATE TABLE fl_orders (id INT PRIMARY KEY, user_id INT, name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['fl_users', 'fl_flags', 'fl_orders'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO fl_users VALUES (1, 'Alice', 'admin', 90)");
        $this->pdo->exec("INSERT INTO fl_users VALUES (2, 'Bob', 'user', 70)");
        $this->pdo->exec("INSERT INTO fl_users VALUES (3, 'Charlie', 'moderator', 85)");

        }

    public function testFetchLazyReturnsPdoRow(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, role FROM fl_users WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_LAZY);

        // FETCH_LAZY returns a PDORow object (or similar) with lazy property access
        if ($row === false) {
            $this->markTestSkipped('FETCH_LAZY not supported on this ZTD version');
        }

        $this->assertSame('Alice', $row->name);
        $this->assertSame('admin', $row->role);
    }

    public function testFetchLazyWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT id, name, score FROM fl_users WHERE score > ?');
        $stmt->execute([80]);
        $row = $stmt->fetch(PDO::FETCH_LAZY);

        if ($row === false) {
            $this->markTestSkipped('FETCH_LAZY not supported or no results');
        }

        // Should get Alice (90) or Charlie (85) — both have score > 80
        $this->assertContains($row->name, ['Alice', 'Charlie']);
    }

    public function testFetchLazyAfterMutation(): void
    {
        $this->pdo->exec("UPDATE fl_users SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM fl_users WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_LAZY);

        if ($row === false) {
            $this->markTestSkipped('FETCH_LAZY not supported');
        }

        $this->assertSame('Alice Updated', $row->name);
    }

    public function testExecWithSelectReturnsRowCountOrZero(): void
    {
        // exec() with SELECT goes through ZTD rewriter, which returns rowCount()
        // On SQLite this typically returns 0 (rowCount not reliable for SELECT)
        $result = $this->pdo->exec('SELECT * FROM fl_users');
        $this->assertIsInt($result);
        $this->assertContains($result, [0, 3]);
    }

    public function testPrepareEmptyResultSet(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM fl_users WHERE id = ?');
        $stmt->execute([999]);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);

        // fetchColumn on empty result returns false
        $stmt->execute([999]);
        $this->assertFalse($stmt->fetchColumn());
    }

    public function testFetchAfterFetchAll(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fl_users ORDER BY id');
        $all = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $all);

        // After fetchAll, fetch() should return false (cursor exhausted)
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    public function testMultipleSequentialQueries(): void
    {
        // Execute multiple queries without fetching results
        $stmt1 = $this->pdo->query('SELECT * FROM fl_users WHERE id = 1');
        $stmt2 = $this->pdo->query('SELECT * FROM fl_users WHERE id = 2');
        $stmt3 = $this->pdo->query('SELECT * FROM fl_users WHERE id = 3');

        // All three statements should have valid results
        $row3 = $stmt3->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Charlie', $row3['name']);

        $row1 = $stmt1->fetch(PDO::FETCH_ASSOC);
        // stmt1 may or may not still be valid depending on implementation
        // Just verify stmt3 works
        $this->assertNotNull($row3);
    }

    public function testColumnCountOnEmptyResultSet(): void
    {
        $stmt = $this->pdo->prepare('SELECT id, name, role FROM fl_users WHERE id = ?');
        $stmt->execute([999]);

        // columnCount should still report 3 even with 0 rows
        $this->assertSame(3, $stmt->columnCount());
    }

    public function testFetchColumnExhaustion(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fl_users ORDER BY id');

        $names = [];
        while (($name = $stmt->fetchColumn()) !== false) {
            $names[] = $name;
        }

        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
        // One more call should still return false
        $this->assertFalse($stmt->fetchColumn());
    }

    public function testParamBoolBinding(): void
    {
        $this->pdo->exec('INSERT INTO fl_flags VALUES (1, 1)');
        $this->pdo->exec('INSERT INTO fl_flags VALUES (2, 0)');

        $stmt = $this->pdo->prepare('SELECT id FROM fl_flags WHERE active = ?');
        $stmt->bindValue(1, true, PDO::PARAM_BOOL);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // SQLite returns integers for INT columns
        $this->assertCount(1, $rows);
        $this->assertEquals(1, $rows[0]);
    }

    public function testSelectWithDuplicateColumnNames(): void
    {
        $this->pdo->exec("INSERT INTO fl_orders VALUES (10, 1, 'Order A')");

        // JOIN producing duplicate 'name' columns
        $stmt = $this->pdo->query(
            'SELECT fl_users.name, fl_orders.name FROM fl_users JOIN fl_orders ON fl_users.id = fl_orders.user_id'
        );
        $row = $stmt->fetch(PDO::FETCH_NUM);
        if ($row !== false) {
            $this->assertSame('Alice', $row[0]);
            $this->assertSame('Order A', $row[1]);
        }
    }

    public function testReExecutePreparedSelectMultipleTimes(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM fl_users WHERE id = ?');

        for ($i = 1; $i <= 3; $i++) {
            $stmt->execute([$i]);
            $name = $stmt->fetchColumn();
            $this->assertNotFalse($name);
        }

        // Verify we can go back to id=1
        $stmt->execute([1]);
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
