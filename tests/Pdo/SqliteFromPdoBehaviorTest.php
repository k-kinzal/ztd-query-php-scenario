<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests behavioral differences between ZtdPdo::fromPdo() and new ZtdPdo() constructor
 * on SQLite. Documents how schema reflection timing affects ZTD operations.
 * @spec SPEC-1.4
 */
class SqliteFromPdoBehaviorTest extends TestCase
{
    public function testFromPdoReflectsExistingSchema(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE test_reflect (id INTEGER PRIMARY KEY, val TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO test_reflect (id, val) VALUES (1, 'hello')");
        $pdo->exec("UPDATE test_reflect SET val = 'world' WHERE id = 1");

        $stmt = $pdo->query('SELECT val FROM test_reflect WHERE id = 1');
        $this->assertSame('world', $stmt->fetch(PDO::FETCH_ASSOC)['val']);
    }

    public function testFromPdoCannotUpdateUnreflectedTable(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        // Create table AFTER fromPdo() — schema not reflected
        $pdo = ZtdPdo::fromPdo($raw);
        $raw->exec('CREATE TABLE late_table (id INTEGER PRIMARY KEY, val TEXT)');

        // INSERT works (doesn't need PK info)
        $pdo->exec("INSERT INTO late_table (id, val) VALUES (1, 'hello')");

        // UPDATE fails — requires PK info from schema reflection
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/requires primary keys/i');
        $pdo->exec("UPDATE late_table SET val = 'world' WHERE id = 1");
    }

    public function testFromPdoDeleteOnUnreflectedTableDoesNotAffectPhysical(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE late_del (id INTEGER PRIMARY KEY, val TEXT)');
        $raw->exec("INSERT INTO late_del (id, val) VALUES (1, 'physical')");

        $pdo = ZtdPdo::fromPdo($raw);

        // DELETE on unreflected table via fromPdo() — does not throw, does not affect physical DB
        $pdo->exec("DELETE FROM late_del WHERE id = 1");

        // Physical data is unchanged
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT COUNT(*) as c FROM late_del');
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testSelectOnUnreflectedTableReturnsEmpty(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE existing (id INTEGER PRIMARY KEY, val TEXT)');
        $raw->exec("INSERT INTO existing (id, val) VALUES (1, 'physical')");

        $pdo = ZtdPdo::fromPdo($raw);

        // SELECT on reflected table with no shadow data returns empty (CTE replaces table)
        $stmt = $pdo->query('SELECT * FROM existing');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);

        // After inserting shadow data, SELECT returns shadow data only
        $pdo->exec("INSERT INTO existing (id, val) VALUES (2, 'shadow')");
        $stmt = $pdo->query('SELECT * FROM existing');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);
    }

    public function testDisableZtdShowsPhysicalData(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE toggle_test (id INTEGER PRIMARY KEY, val TEXT)');
        $raw->exec("INSERT INTO toggle_test (id, val) VALUES (1, 'physical')");

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO toggle_test (id, val) VALUES (2, 'shadow')");

        // ZTD enabled: only shadow data
        $stmt = $pdo->query('SELECT * FROM toggle_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        // ZTD disabled: only physical data
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM toggle_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testMultipleTablesReflected(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)');
        $raw->exec('CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER)');

        $pdo = ZtdPdo::fromPdo($raw);

        // Both tables are reflected — CRUD works on both
        $pdo->exec("INSERT INTO authors (id, name) VALUES (1, 'Tolkien')");
        $pdo->exec("INSERT INTO books (id, title, author_id) VALUES (1, 'The Hobbit', 1)");

        // JOIN across shadow tables
        $stmt = $pdo->query("SELECT b.title, a.name FROM books b JOIN authors a ON b.author_id = a.id");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('The Hobbit', $row['title']);
        $this->assertSame('Tolkien', $row['name']);

        // UPDATE both
        $pdo->exec("UPDATE authors SET name = 'J.R.R. Tolkien' WHERE id = 1");
        $pdo->exec("UPDATE books SET title = 'The Lord of the Rings' WHERE id = 1");

        $stmt = $pdo->query("SELECT b.title, a.name FROM books b JOIN authors a ON b.author_id = a.id");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('The Lord of the Rings', $row['title']);
        $this->assertSame('J.R.R. Tolkien', $row['name']);

        // Nothing leaked
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT COUNT(*) as c FROM authors');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }
}
