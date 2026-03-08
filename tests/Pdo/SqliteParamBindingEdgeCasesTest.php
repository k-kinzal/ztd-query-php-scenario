<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statement parameter binding edge cases — mixed param types,
 * named vs positional params, rebinding, NULL binding, and type coercion.
 * @spec SPEC-3.2
 */
class SqliteParamBindingEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pb_items (id INTEGER PRIMARY KEY, name TEXT, price REAL, active INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['pb_items'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pb_items (id, name, price, active) VALUES (1, 'Widget', 10.50, 1)");
        $this->pdo->exec("INSERT INTO pb_items (id, name, price, active) VALUES (2, 'Gadget', 25.00, 0)");
        $this->pdo->exec("INSERT INTO pb_items (id, name, price, active) VALUES (3, 'Doohickey', 5.75, 1)");
    }
    public function testPositionalParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE price > ? AND active = ?');
        $stmt->execute([8.0, 1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }

    public function testNamedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE price > :min_price AND active = :active');
        $stmt->execute([':min_price' => 8.0, ':active' => 1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }

    public function testBindValueWithExplicitTypes(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE id = ?');
        $stmt->bindValue(1, 2, PDO::PARAM_INT);
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Gadget', $row['name']);
    }

    public function testBindValueStringType(): void
    {
        $stmt = $this->pdo->prepare('SELECT id FROM pb_items WHERE name = ?');
        $stmt->bindValue(1, 'Widget', PDO::PARAM_STR);
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['id']);
    }

    public function testBindParamByReference(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE id = ?');
        $id = 1;
        $stmt->bindParam(1, $id, PDO::PARAM_INT);
        $stmt->execute();
        $this->assertSame('Widget', $stmt->fetch(PDO::FETCH_ASSOC)['name']);

        // Change reference and re-execute
        $id = 3;
        $stmt->execute();
        $this->assertSame('Doohickey', $stmt->fetch(PDO::FETCH_ASSOC)['name']);
    }

    public function testNullParamBinding(): void
    {
        $this->pdo->exec("INSERT INTO pb_items (id, name, price, active) VALUES (4, NULL, 0, 1)");

        $stmt = $this->pdo->prepare('SELECT id FROM pb_items WHERE name IS NULL');
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(4, (int) $rows[0]['id']);
    }

    public function testBindValueNullType(): void
    {
        // Prepared INSERT with NULL parameter
        $stmt = $this->pdo->prepare('INSERT INTO pb_items (id, name, price, active) VALUES (?, ?, ?, ?)');
        $stmt->bindValue(1, 5, PDO::PARAM_INT);
        $stmt->bindValue(2, null, PDO::PARAM_NULL);
        $stmt->bindValue(3, 0.0, PDO::PARAM_STR);
        $stmt->bindValue(4, 1, PDO::PARAM_INT);
        $stmt->execute();

        $stmt = $this->pdo->query('SELECT name FROM pb_items WHERE id = 5');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['name']);
    }

    public function testReExecuteWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE active = ?');

        $stmt->execute([1]);
        $active = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $active); // Widget, Doohickey

        $stmt->execute([0]);
        $inactive = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $inactive); // Gadget
        $this->assertSame('Gadget', $inactive[0]['name']);
    }

    public function testPreparedInsertThenPreparedSelect(): void
    {
        // Insert first
        $insertStmt = $this->pdo->prepare('INSERT INTO pb_items (id, name, price, active) VALUES (?, ?, ?, ?)');
        $insertStmt->execute([10, 'NewItem', 99.99, 1]);

        // Prepare SELECT AFTER insert — CTE snapshot includes the new row
        $selectStmt = $this->pdo->prepare('SELECT name, price FROM pb_items WHERE id = ?');
        $selectStmt->execute([10]);
        $row = $selectStmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('NewItem', $row['name']);
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.01);
    }

    public function testPreparedSelectBeforeInsertReturnsEmpty(): void
    {
        // Prepare SELECT before insert — CTE snapshot is empty
        $selectStmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE id = ?');

        // Insert data after SELECT was prepared
        $this->pdo->exec("INSERT INTO pb_items (id, name, price, active) VALUES (20, 'Late', 1.0, 1)");

        // The prepared SELECT has a CTE from prepare time (no id=20) → no results
        $selectStmt->execute([20]);
        $row = $selectStmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    public function testPreparedUpdateWithParams(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pb_items SET price = ? WHERE id = ?');
        $stmt->execute([99.99, 1]);

        $select = $this->pdo->query('SELECT price FROM pb_items WHERE id = 1');
        $this->assertEqualsWithDelta(99.99, (float) $select->fetch(PDO::FETCH_ASSOC)['price'], 0.01);

        // Re-execute for different row
        $stmt->execute([0.01, 2]);
        $select = $this->pdo->query('SELECT price FROM pb_items WHERE id = 2');
        $this->assertEqualsWithDelta(0.01, (float) $select->fetch(PDO::FETCH_ASSOC)['price'], 0.01);
    }

    public function testPreparedDeleteWithParams(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM pb_items WHERE active = ?');
        $stmt->execute([0]);

        $select = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pb_items');
        $this->assertSame(2, (int) $select->fetch(PDO::FETCH_ASSOC)['cnt']);
    }
}
