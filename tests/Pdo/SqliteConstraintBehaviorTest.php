<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/** @spec SPEC-8.1 */
class SqliteConstraintBehaviorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE constraint_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)';
    }

    protected function getTableNames(): array
    {
        return ['constraint_test'];
    }


    public function testDuplicatePrimaryKeyNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Bob', 'bob@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testNotNullNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, NULL, 'test@test.com')");

        $stmt = $this->pdo->query('SELECT * FROM constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
    }

    public function testUniqueConstraintNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Alice', 'same@email.com')");
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (2, 'Bob', 'same@email.com')");

        $stmt = $this->pdo->query('SELECT * FROM constraint_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }
}
