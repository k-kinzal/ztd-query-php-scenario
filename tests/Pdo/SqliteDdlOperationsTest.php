<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

class SqliteDdlOperationsTest extends TestCase
{
    private PDO $raw;
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE ddl_existing (id INTEGER PRIMARY KEY, val TEXT)');

        $this->pdo = ZtdPdo::fromPdo($this->raw);
    }

    public function testCreateTableThrowsWhenTableExistsPhysically(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/already exists/i');

        $this->pdo->exec('CREATE TABLE ddl_existing (id INTEGER PRIMARY KEY)');
    }

    public function testCreateTableInShadowWhenNotPhysical(): void
    {
        $this->pdo->exec('CREATE TABLE ddl_new (id INTEGER PRIMARY KEY, name TEXT)');

        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (1, 'shadow')");

        $stmt = $this->pdo->query('SELECT * FROM ddl_new WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame('shadow', $rows[0]['name']);
    }

    public function testDropTableClearsShadowData(): void
    {
        $this->pdo->exec("INSERT INTO ddl_existing (id, val) VALUES (1, 'shadow')");

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);

        $this->pdo->exec('DROP TABLE ddl_existing');

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testUpdateOnShadowCreatedTable(): void
    {
        $this->pdo->exec('CREATE TABLE ddl_new (id INTEGER PRIMARY KEY, name TEXT)');
        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (1, 'original')");
        $this->pdo->exec("UPDATE ddl_new SET name = 'updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM ddl_new WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $rows[0]['name']);
    }

    public function testDeleteOnShadowCreatedTable(): void
    {
        $this->pdo->exec('CREATE TABLE ddl_new (id INTEGER PRIMARY KEY, name TEXT)');
        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO ddl_new (id, name) VALUES (2, 'world')");
        $this->pdo->exec('DELETE FROM ddl_new WHERE id = 1');

        $stmt = $this->pdo->query('SELECT * FROM ddl_new');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    public function testDeleteWithWhereDeletesShadowedRows(): void
    {
        $this->pdo->exec("INSERT INTO ddl_existing (id, val) VALUES (1, 'a')");
        $this->pdo->exec("INSERT INTO ddl_existing (id, val) VALUES (2, 'b')");

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing');
        $this->assertCount(2, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // DELETE with WHERE clause removes specific rows from shadow store
        $this->pdo->exec('DELETE FROM ddl_existing WHERE id = 1');

        $stmt = $this->pdo->query('SELECT * FROM ddl_existing');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('b', $rows[0]['val']);
    }
}
