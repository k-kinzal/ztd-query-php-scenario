<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class SqliteSchemaReflectionTest extends TestCase
{
    public function testAdapterConstructedAfterTableReflectsSchema(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE reflect_test (id INTEGER PRIMARY KEY, val TEXT)');

        // Wrap after table exists → schema reflected
        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO reflect_test (id, val) VALUES (1, 'original')");
        $pdo->exec("UPDATE reflect_test SET val = 'updated' WHERE id = 1");

        $stmt = $pdo->query('SELECT val FROM reflect_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $rows[0]['val']);

        $pdo->exec("DELETE FROM reflect_test WHERE id = 1");
        $stmt = $pdo->query('SELECT * FROM reflect_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testUpdateFailsWhenSchemaNotReflected(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        // Wrap BEFORE table exists → schema NOT reflected
        $pdo = ZtdPdo::fromPdo($raw);

        // Create table after adapter
        $raw->exec('CREATE TABLE reflect_test (id INTEGER PRIMARY KEY, val TEXT)');
        $raw->exec("INSERT INTO reflect_test VALUES (1, 'physical')");

        // INSERT works (doesn't need primary key info)
        $pdo->exec("INSERT INTO reflect_test (id, val) VALUES (2, 'shadow')");

        // UPDATE fails because schema was not reflected
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/requires primary keys/i');
        $pdo->exec("UPDATE reflect_test SET val = 'updated' WHERE id = 1");
    }
}
