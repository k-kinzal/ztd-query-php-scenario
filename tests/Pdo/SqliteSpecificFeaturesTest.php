<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite-specific features: typeof(), instr(), printf(), IIF(), hex/unhex, group_concat order.
 * @spec pending
 */
class SqliteSpecificFeaturesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL, code TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['items'];
    }


    public function testTypeof(): void
    {
        $stmt = $this->pdo->query("SELECT typeof(name) AS name_type, typeof(price) AS price_type, typeof(id) AS id_type FROM items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('text', $row['name_type']);
        $this->assertSame('real', $row['price_type']);
        $this->assertSame('integer', $row['id_type']);
    }

    public function testInstr(): void
    {
        $stmt = $this->pdo->query("SELECT INSTR(name, 'id') AS pos FROM items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['pos']); // 'Widget' → 'id' at position 2
    }

    public function testIif(): void
    {
        $stmt = $this->pdo->query("SELECT name, IIF(price > 15, 'expensive', 'cheap') AS tier FROM items ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('cheap', $rows[0]['tier']);
        $this->assertSame('expensive', $rows[1]['tier']);
    }

    public function testPrintf(): void
    {
        $stmt = $this->pdo->query("SELECT printf('%.2f', price) AS formatted FROM items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('9.99', $row['formatted']);
    }

    public function testHex(): void
    {
        $stmt = $this->pdo->query("SELECT HEX(code) AS hex_code FROM items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('57303031', $row['hex_code']); // W001 in hex
    }

    public function testGroupConcatWithOrder(): void
    {
        // SQLite GROUP_CONCAT doesn't guarantee order, but we can test it works
        $stmt = $this->pdo->query("SELECT GROUP_CONCAT(name, '; ') AS names FROM items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertStringContainsString('Widget', $row['names']);
        $this->assertStringContainsString('Gadget', $row['names']);
        $this->assertStringContainsString('Gizmo', $row['names']);
    }

    public function testCast(): void
    {
        $stmt = $this->pdo->query("SELECT CAST(price AS INTEGER) AS int_price FROM items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(9, (int) $row['int_price']);
    }

    public function testNullif(): void
    {
        $this->pdo->exec("INSERT INTO items (id, name, price, code) VALUES (4, '', 0.00, '')");

        $stmt = $this->pdo->query("SELECT NULLIF(name, '') AS name_or_null FROM items WHERE id = 4");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['name_or_null']);
    }

    public function testMinMaxOnMixedTypes(): void
    {
        $stmt = $this->pdo->query("SELECT MIN(id) AS min_id, MAX(id) AS max_id, MIN(price) AS min_price, MAX(price) AS max_price FROM items");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['min_id']);
        $this->assertSame(3, (int) $row['max_id']);
        $this->assertEqualsWithDelta(9.99, (float) $row['min_price'], 0.01);
        $this->assertEqualsWithDelta(29.99, (float) $row['max_price'], 0.01);
    }
}
