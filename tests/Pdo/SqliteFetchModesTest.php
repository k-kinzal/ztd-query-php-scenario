<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests various PDO fetch modes with ZTD shadow operations on SQLite.
 *
 * Ensures FETCH_OBJ, FETCH_NUM, FETCH_BOTH, FETCH_COLUMN,
 * fetchObject(), and fetchAll() modes work correctly with shadow data.
 * @spec SPEC-3.4
 */
class SqliteFetchModesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_fm_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sl_fm_test'];
    }


    /**
     * FETCH_OBJ returns stdClass objects.
     */
    public function testFetchObj(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM sl_fm_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_OBJ);
        $this->assertIsObject($row);
        $this->assertSame('Alice', $row->name);
    }

    /**
     * FETCH_NUM returns numeric-indexed array.
     */
    public function testFetchNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM sl_fm_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_NUM);
        $this->assertIsArray($row);
        $this->assertSame('Alice', $row[1]);
    }

    /**
     * FETCH_BOTH returns both associative and numeric keys.
     */
    public function testFetchBoth(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM sl_fm_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_BOTH);
        $this->assertSame($row['name'], $row[1]);
    }

    /**
     * fetchAll with FETCH_COLUMN.
     */
    public function testFetchAllColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM sl_fm_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    /**
     * fetchObject returns typed object.
     */
    public function testFetchObject(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM sl_fm_test WHERE id = 2');
        $obj = $stmt->fetchObject();
        $this->assertIsObject($obj);
        $this->assertSame('Bob', $obj->name);
    }

    /**
     * fetchAll with FETCH_OBJ.
     */
    public function testFetchAllObj(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM sl_fm_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_OBJ);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]->name);
    }

    /**
     * fetchAll with FETCH_KEY_PAIR.
     */
    public function testFetchAllKeyPair(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM sl_fm_test ORDER BY id');
        $pairs = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
        $this->assertSame([1 => 'Alice', 2 => 'Bob', 3 => 'Charlie'], $pairs);
    }

    /**
     * Fetch modes work after shadow mutation.
     */
    public function testFetchModesAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_fm_test VALUES (4, 'Diana', 88)");

        $stmt = $this->pdo->query('SELECT name FROM sl_fm_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(4, $names);
        $this->assertSame('Diana', $names[3]);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_fm_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
