<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests ALTER TABLE operations in ZTD mode on MySQL via PDO.
 *
 * Unlike SQLite (where CTE rewriter ignores schema changes), MySQL fully
 * supports ALTER TABLE in the shadow schema.
 * @spec SPEC-5.1a
 */
class MysqlAlterTableTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_pdo_alter_test (id INT PRIMARY KEY, name VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_pdo_alter_test'];
    }


    public function testAddColumn(): void
    {
        $this->pdo->exec('ALTER TABLE mysql_pdo_alter_test ADD COLUMN age INT');

        $this->pdo->exec("INSERT INTO mysql_pdo_alter_test (id, name, age) VALUES (3, 'Charlie', 30)");

        // On MySQL, ALTER TABLE fully updates the shadow schema — new column is visible
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertArrayHasKey('age', $rows[0]);
        $this->assertSame(30, (int) $rows[0]['age']);
    }

    public function testDropColumn(): void
    {
        $this->pdo->exec('ALTER TABLE mysql_pdo_alter_test DROP COLUMN name');
        $this->pdo->exec("INSERT INTO mysql_pdo_alter_test (id) VALUES (3)");

        // On MySQL, dropped column is removed from results
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertArrayNotHasKey('name', $rows[0]);
    }

    public function testRenameColumn(): void
    {
        $this->pdo->exec('ALTER TABLE mysql_pdo_alter_test RENAME COLUMN name TO full_name');
        $this->pdo->exec("INSERT INTO mysql_pdo_alter_test (id, full_name) VALUES (3, 'Charlie')");

        // On MySQL, renamed column is reflected in results
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertArrayHasKey('full_name', $rows[0]);
        $this->assertArrayNotHasKey('name', $rows[0]);
        $this->assertSame('Charlie', $rows[0]['full_name']);
    }

    public function testModifyColumn(): void
    {
        $this->pdo->exec('ALTER TABLE mysql_pdo_alter_test MODIFY COLUMN name TEXT');

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_alter_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testChangeColumn(): void
    {
        $this->pdo->exec('ALTER TABLE mysql_pdo_alter_test CHANGE COLUMN name full_name VARCHAR(500)');

        $this->pdo->exec("INSERT INTO mysql_pdo_alter_test (id, full_name) VALUES (3, 'Charlie Brown')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Charlie Brown', $rows[0]['full_name']);
    }

    public function testAlterTableIsolation(): void
    {
        $this->pdo->exec('ALTER TABLE mysql_pdo_alter_test ADD COLUMN extra VARCHAR(255)');

        // Physical table should not be altered
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_alter_test LIMIT 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertArrayNotHasKey('extra', $row);
    }
}
