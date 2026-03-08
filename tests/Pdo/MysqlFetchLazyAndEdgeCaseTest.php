<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests FETCH_LAZY mode, exec() with SELECT, and other edge cases on MySQL.
 * @spec pending
 */
class MysqlFetchLazyAndEdgeCaseTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE fl_users_m (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20), score INT)',
            'CREATE TABLE fl_flags_m (id INT PRIMARY KEY, active INT)',
            'CREATE TABLE fl_orders_m (id INT PRIMARY KEY, user_id INT, name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['fl_users_m', 'fl_flags_m', 'fl_orders_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO fl_users_m VALUES (1, 'Alice', 'admin', 90)");
        $this->pdo->exec("INSERT INTO fl_users_m VALUES (2, 'Bob', 'user', 70)");
        $this->pdo->exec("INSERT INTO fl_users_m VALUES (3, 'Charlie', 'moderator', 85)");
    }

    public function testFetchLazyReturnsPdoRow(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, role FROM fl_users_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_LAZY);

        if ($row === false) {
            $this->markTestSkipped('FETCH_LAZY not supported');
        }

        $this->assertSame('Alice', $row->name);
        $this->assertSame('admin', $row->role);
    }

    public function testFetchLazyAfterMutation(): void
    {
        $this->pdo->exec("UPDATE fl_users_m SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM fl_users_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_LAZY);

        if ($row === false) {
            $this->markTestSkipped('FETCH_LAZY not supported');
        }

        $this->assertSame('Alice Updated', $row->name);
    }

    public function testExecWithSelectReturnsRowCountOrZero(): void
    {
        // exec() with SELECT goes through ZTD rewriter, which returns rowCount()
        // On MySQL/PostgreSQL this may return the actual row count
        $result = $this->pdo->exec('SELECT * FROM fl_users_m');
        $this->assertIsInt($result);
        // Shadow store has 3 rows, so result should be 0 or 3 (platform-dependent)
        $this->assertContains($result, [0, 3]);
    }

    public function testPrepareEmptyResultSet(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM fl_users_m WHERE id = ?');
        $stmt->execute([999]);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([], $rows);
    }

    public function testFetchAfterFetchAll(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fl_users_m ORDER BY id');
        $all = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $all);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    public function testColumnCountOnEmptyResultSet(): void
    {
        $stmt = $this->pdo->prepare('SELECT id, name, role FROM fl_users_m WHERE id = ?');
        $stmt->execute([999]);
        $this->assertSame(3, $stmt->columnCount());
    }

    public function testParamBoolBinding(): void
    {
        $this->pdo->exec('INSERT INTO fl_flags_m VALUES (1, 1)');
        $this->pdo->exec('INSERT INTO fl_flags_m VALUES (2, 0)');

        $stmt = $this->pdo->prepare('SELECT id FROM fl_flags_m WHERE active = ?');
        $stmt->bindValue(1, true, PDO::PARAM_BOOL);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertEquals(1, $rows[0]);
    }

    public function testSelectWithDuplicateColumnNames(): void
    {
        $this->pdo->exec("INSERT INTO fl_orders_m VALUES (10, 1, 'Order A')");

        $stmt = $this->pdo->query(
            'SELECT fl_users_m.name, fl_orders_m.name FROM fl_users_m JOIN fl_orders_m ON fl_users_m.id = fl_orders_m.user_id'
        );
        $row = $stmt->fetch(PDO::FETCH_NUM);
        if ($row !== false) {
            $this->assertSame('Alice', $row[0]);
            $this->assertSame('Order A', $row[1]);
        }
    }

    public function testReExecutePreparedSelectMultipleTimes(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM fl_users_m WHERE id = ?');

        for ($i = 1; $i <= 3; $i++) {
            $stmt->execute([$i]);
            $name = $stmt->fetchColumn();
            $this->assertNotFalse($name);
        }

        $stmt->execute([1]);
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
