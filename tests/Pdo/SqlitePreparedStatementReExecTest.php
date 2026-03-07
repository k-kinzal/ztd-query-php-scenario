<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared statement re-execution patterns on SQLite PDO.
 * Focuses on cursor management, parameter rebinding, and data isolation
 * between executions.
 */
class SqlitePreparedStatementReExecTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(20), price INT)');
        $this->pdo->exec("INSERT INTO items VALUES (1, 'Widget', 'A', 100)");
        $this->pdo->exec("INSERT INTO items VALUES (2, 'Gadget', 'B', 200)");
        $this->pdo->exec("INSERT INTO items VALUES (3, 'Gizmo', 'A', 150)");
        $this->pdo->exec("INSERT INTO items VALUES (4, 'Doohickey', 'B', 50)");
        $this->pdo->exec("INSERT INTO items VALUES (5, 'Thingamajig', 'C', 300)");
    }

    public function testReExecuteSelectWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM items WHERE category = ? ORDER BY name');

        $stmt->execute(['A']);
        $rowsA = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Gizmo', 'Widget'], $rowsA);

        $stmt->execute(['B']);
        $rowsB = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Doohickey', 'Gadget'], $rowsB);

        $stmt->execute(['C']);
        $rowsC = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Thingamajig'], $rowsC);
    }

    public function testReExecuteInsertMultipleTimes(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO items (id, name, category, price) VALUES (?, ?, ?, ?)');
        $stmt->execute([6, 'Extra1', 'D', 10]);
        $stmt->execute([7, 'Extra2', 'D', 20]);
        $stmt->execute([8, 'Extra3', 'D', 30]);

        $count = $this->pdo->query('SELECT COUNT(*) FROM items WHERE category = \'D\'')->fetchColumn();
        $this->assertSame(3, (int) $count);
    }

    public function testReExecuteUpdateWithDifferentValues(): void
    {
        $stmt = $this->pdo->prepare('UPDATE items SET price = ? WHERE id = ?');
        $stmt->execute([999, 1]);
        $stmt->execute([888, 2]);

        $row1 = $this->pdo->query('SELECT price FROM items WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(999, (int) $row1['price']);

        $row2 = $this->pdo->query('SELECT price FROM items WHERE id = 2')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(888, (int) $row2['price']);
    }

    public function testBindValueThenReExecute(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM items WHERE price > :min_price ORDER BY name');

        $stmt->bindValue(':min_price', 100, PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $rows); // Gadget (200), Gizmo (150), Thingamajig (300)

        $stmt->bindValue(':min_price', 250, PDO::PARAM_INT);
        $stmt->execute();
        $rows2 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows2); // Thingamajig (300)
    }

    public function testPreparedSelectSeesEarlierPreparedInsert(): void
    {
        // Insert via prepared statement
        $insert = $this->pdo->prepare('INSERT INTO items (id, name, category, price) VALUES (?, ?, ?, ?)');
        $insert->execute([6, 'NewItem', 'A', 500]);

        // New prepared SELECT should see the inserted row
        $select = $this->pdo->prepare('SELECT name FROM items WHERE category = ? ORDER BY name');
        $select->execute(['A']);
        $rows = $select->fetchAll(PDO::FETCH_COLUMN);
        $this->assertContains('NewItem', $rows);
    }

    public function testReExecuteDeleteWithDifferentTargets(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM items WHERE id = ?');
        $stmt->execute([1]);
        $stmt->execute([3]);

        $count = $this->pdo->query('SELECT COUNT(*) FROM items')->fetchColumn();
        $this->assertSame(3, (int) $count); // 5 - 2 = 3
    }

    public function testMixedPreparedAndExecOperations(): void
    {
        // Prepared insert
        $stmt = $this->pdo->prepare('INSERT INTO items (id, name, category, price) VALUES (?, ?, ?, ?)');
        $stmt->execute([6, 'PrepItem', 'X', 100]);

        // exec insert
        $this->pdo->exec("INSERT INTO items (id, name, category, price) VALUES (7, 'ExecItem', 'X', 200)");

        // Prepared select
        $select = $this->pdo->prepare('SELECT name FROM items WHERE category = ? ORDER BY name');
        $select->execute(['X']);
        $rows = $select->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $rows);
        $this->assertContains('PrepItem', $rows);
        $this->assertContains('ExecItem', $rows);
    }

    public function testPartialFetchThenReExecute(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM items ORDER BY id');
        $stmt->execute();
        $stmt->fetch(PDO::FETCH_ASSOC); // fetch only first row
        // Don't fetch all — re-execute with new params
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(5, $rows); // All rows visible on re-execute
    }
}
