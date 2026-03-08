<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Prepared statements with IN-list parameters on SQLite via PDO.
 * @spec SPEC-3.2
 */
class SqlitePreparedInListTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_il_items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)';
    }

    protected function getTableNames(): array
    {
        return ['sl_il_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_il_items VALUES (1, 'Widget', 'tools', 10.00)");
        $this->pdo->exec("INSERT INTO sl_il_items VALUES (2, 'Gadget', 'tech', 25.00)");
        $this->pdo->exec("INSERT INTO sl_il_items VALUES (3, 'Doohickey', 'tools', 5.00)");
        $this->pdo->exec("INSERT INTO sl_il_items VALUES (4, 'Thingamajig', 'misc', 15.00)");
        $this->pdo->exec("INSERT INTO sl_il_items VALUES (5, 'Whatchamacallit', 'tech', 35.00)");
    }

    public function testSelectWithTwoIdInList(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE id IN (?, ?) ORDER BY id',
            [1, 3]
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame('Doohickey', $rows[1]['name']);
    }

    public function testSelectWithStringInList(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE category IN (?, ?) ORDER BY id',
            ['tools', 'misc']
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }

    public function testSelectNotInList(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE id NOT IN (?, ?, ?) ORDER BY id',
            [1, 2, 3]
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Thingamajig', $rows[0]['name']);
    }

    public function testInListWithSingleElement(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE id IN (?)',
            [3]
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Doohickey', $rows[0]['name']);
    }

    public function testInListNoMatches(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE id IN (?, ?)',
            [99, 100]
        );
        $this->assertCount(0, $rows);
    }

    public function testInListWithAdditionalWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE id IN (?, ?, ?) AND price > ? ORDER BY id',
            [1, 2, 5, 20.00]
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertSame('Whatchamacallit', $rows[1]['name']);
    }

    public function testInListAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM sl_il_items WHERE id = 2");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE id IN (?, ?, ?) ORDER BY id',
            [1, 2, 3]
        );
        $this->assertCount(2, $rows);
    }

    public function testInListAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE sl_il_items SET category = 'tech' WHERE id = 1");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_il_items WHERE category IN (?) ORDER BY id',
            ['tech']
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }
}
