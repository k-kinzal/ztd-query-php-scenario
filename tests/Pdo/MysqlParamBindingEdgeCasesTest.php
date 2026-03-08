<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statement parameter binding edge cases on MySQL PDO.
 * @spec SPEC-3.2
 */
class MysqlParamBindingEdgeCasesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pb_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), active TINYINT)';
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

    public function testBindParamByReference(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE id = ?');
        $id = 1;
        $stmt->bindParam(1, $id, PDO::PARAM_INT);
        $stmt->execute();
        $this->assertSame('Widget', $stmt->fetch(PDO::FETCH_ASSOC)['name']);

        $id = 3;
        $stmt->execute();
        $this->assertSame('Doohickey', $stmt->fetch(PDO::FETCH_ASSOC)['name']);
    }

    public function testPreparedSelectBeforeInsertReturnsEmpty(): void
    {
        $selectStmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE id = ?');

        $this->pdo->exec("INSERT INTO pb_items (id, name, price, active) VALUES (20, 'Late', 1.0, 1)");

        // CTE snapshot from prepare time → no id=20
        $selectStmt->execute([20]);
        $row = $selectStmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    public function testReExecuteWithDifferentParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pb_items WHERE active = ?');

        $stmt->execute([1]);
        $this->assertCount(2, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $stmt->execute([0]);
        $inactive = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $inactive);
        $this->assertSame('Gadget', $inactive[0]['name']);
    }
}
