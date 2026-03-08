<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with DEFAULT keyword on SQLite PDO ZTD.
 *
 * SQLite supports:
 *   INSERT INTO t DEFAULT VALUES
 *   INSERT INTO t (col) VALUES (DEFAULT)  — NOT supported by SQLite itself
 *
 * "INSERT ... DEFAULT VALUES" fails under ZTD because InsertTransformer
 * requires explicit values to project into the CTE.
 * This is already documented; this test verifies and extends coverage.
 * @spec SPEC-4.1
 */
class SqliteInsertDefaultValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE si_def_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT \\\'default_name\\\',
            score INTEGER DEFAULT 100
        )';
    }

    protected function getTableNames(): array
    {
        return ['si_def_test'];
    }


    /**
     * INSERT ... DEFAULT VALUES should fail under ZTD.
     *
     * SQLite natively supports this, but InsertTransformer cannot project it.
     */
    public function testInsertDefaultValuesFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('INSERT INTO si_def_test DEFAULT VALUES');
    }

    /**
     * INSERT with explicit values works normally.
     */
    public function testInsertWithExplicitValues(): void
    {
        $this->pdo->exec("INSERT INTO si_def_test (name, score) VALUES ('Alice', 90)");

        $stmt = $this->pdo->query('SELECT name, score FROM si_def_test WHERE name = \'Alice\'');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT with NULL (not DEFAULT) as value.
     */
    public function testInsertWithNullValues(): void
    {
        $this->pdo->exec('INSERT INTO si_def_test (name, score) VALUES (NULL, NULL)');

        $stmt = $this->pdo->query('SELECT name, score FROM si_def_test ORDER BY id DESC LIMIT 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['name']);
        $this->assertNull($row['score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO si_def_test (name, score) VALUES ('Bob', 80)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM si_def_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM si_def_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
