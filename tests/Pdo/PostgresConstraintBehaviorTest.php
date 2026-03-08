<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/** @spec SPEC-8.1 */
class PostgresConstraintBehaviorTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE constraint_test (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE)',
            'CREATE TABLE constraint_child (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY (parent_id) REFERENCES constraint_test(id))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['constraint_child', 'constraint_test'];
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

    public function testForeignKeyNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO constraint_child (id, parent_id) VALUES (1, 999)");

        $stmt = $this->pdo->query('SELECT * FROM constraint_child WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(999, (int) $rows[0]['parent_id']);
    }
}
