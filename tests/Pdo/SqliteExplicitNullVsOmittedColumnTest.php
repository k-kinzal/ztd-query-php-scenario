<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with explicit NULL vs omitted columns on SQLite.
 *
 * Default column values are NOT applied in ZTD shadow store.
 * Both explicit NULL and omitted columns result in NULL.
 * @spec pending
 */
class SqliteExplicitNullVsOmittedColumnTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE enoc_test (id INT PRIMARY KEY, name VARCHAR(50) DEFAULT \'default_name\', score INT DEFAULT 100)';
    }

    protected function getTableNames(): array
    {
        return ['enoc_test'];
    }


    /**
     * INSERT with explicit NULL stores NULL.
     */
    public function testExplicitNullStoresNull(): void
    {
        $this->pdo->exec('INSERT INTO enoc_test (id, name, score) VALUES (1, NULL, NULL)');

        $stmt = $this->pdo->query('SELECT name, score FROM enoc_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['name']);
        $this->assertNull($row['score']);
    }

    /**
     * INSERT with omitted columns also results in NULL (not DEFAULT).
     */
    public function testOmittedColumnsAreNull(): void
    {
        $this->pdo->exec('INSERT INTO enoc_test (id) VALUES (1)');

        $stmt = $this->pdo->query('SELECT name, score FROM enoc_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // Default values are NOT applied in shadow store
        $this->assertNull($row['name']);
        $this->assertNull($row['score']);
    }

    /**
     * INSERT with explicit values works correctly.
     */
    public function testExplicitValuesWork(): void
    {
        $this->pdo->exec("INSERT INTO enoc_test VALUES (1, 'Alice', 95)");

        $stmt = $this->pdo->query('SELECT name, score FROM enoc_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * IS NULL comparison works for both cases.
     */
    public function testIsNullComparison(): void
    {
        $this->pdo->exec('INSERT INTO enoc_test (id, name, score) VALUES (1, NULL, NULL)');
        $this->pdo->exec('INSERT INTO enoc_test (id) VALUES (2)');
        $this->pdo->exec("INSERT INTO enoc_test VALUES (3, 'Bob', 80)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM enoc_test WHERE name IS NULL');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM enoc_test WHERE name IS NOT NULL');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }
}
