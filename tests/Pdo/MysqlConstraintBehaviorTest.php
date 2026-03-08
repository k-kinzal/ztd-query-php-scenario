<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests that database constraints are NOT enforced in the shadow store on MySQL via PDO.
 * @spec SPEC-8.1
 */
class MysqlConstraintBehaviorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_constraint_test (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE)',
            'CREATE TABLE mysql_constraint_child (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY (parent_id) REFERENCES mysql_constraint_test(id))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_constraint_test', 'mysql_constraint_child'];
    }


    public function testDuplicatePrimaryKeyNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, 'Bob', 'bob@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testNotNullNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, NULL, 'test@test.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
    }

    public function testUniqueConstraintNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, 'Alice', 'same@email.com')");
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (2, 'Bob', 'same@email.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_constraint_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testForeignKeyNotEnforcedInShadow(): void
    {
        // Shadow store does not enforce FOREIGN KEY constraints
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_constraint_child');
        $raw->exec('CREATE TABLE mysql_constraint_child (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY (parent_id) REFERENCES mysql_constraint_test(id))');

        $this->pdo->exec("INSERT INTO mysql_constraint_child (id, parent_id) VALUES (1, 999)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_constraint_child WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(999, (int) $rows[0]['parent_id']);

        $raw->exec('DROP TABLE IF EXISTS mysql_constraint_child');
    }
}
